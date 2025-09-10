import time
import logging
import os
import pendulum
import requests
import json
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from airflow.models import Variable

# Configura o logger para a DAG
logger = logging.getLogger(__name__)

# --- CLASSE DEDICADA PARA O CLIENTE DO AZURE DATA FACTORY ---
class ADFPipelineClient:
    """
    Cliente para interagir com o Azure Data Factory.
    Centraliza a lógica de autenticação e execução de pipelines.
    """
    def __init__(self, subscription_id, tenant_id, client_id, client_secret):
        self.credential = ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret
        )
        self.subscription_id = subscription_id
        self.client = DataFactoryManagementClient(
            credential=self.credential,
            subscription_id=self.subscription_id
        )

    def run_pipeline(self, resource_group, factory_name, pipeline_name, parameters=None, polling_interval=30):
        logger.info(f"Disparando o pipeline '{pipeline_name}' no ADF '{factory_name}'.")
        run_response = self.client.pipelines.create_run(
            resource_group_name=resource_group,
            factory_name=factory_name,
            pipeline_name=pipeline_name,
            parameters=parameters
        )
        run_id = run_response.run_id
        logger.info(f"Pipeline '{pipeline_name}' iniciado com runId: {run_id}")

        status = "InProgress"
        start_time = time.time()
        
        while status in ["InProgress", "Queued"]:
            time.sleep(polling_interval)
            run_info = self.client.pipeline_runs.get(
                resource_group_name=resource_group,
                factory_name=factory_name,
                run_id=run_id
            )
            status = run_info.status
            logger.info(f"Status atual do pipeline: {status}")

        duration = time.time() - start_time
        logger.info(f"Pipeline '{pipeline_name}' finalizado em {duration:.2f} segundos com status: {status}")

        if status != "Succeeded":
            raise Exception(f"Pipeline '{pipeline_name}' falhou com status: {status}")

# --- FUNÇÃO PARA O OPERADOR DO AZURE ---
def trigger_adf_pipeline(**kwargs):
    pipeline_name = kwargs.get('pipeline_name')
    resource_group = kwargs.get('resource_group')
    factory_name = kwargs.get('factory_name')
    parameters = kwargs.get('parameters')
    polling_interval = kwargs.get('polling_interval', 30)
    
    tenant_id = kwargs.get("azure_tenant_id")
    client_id = kwargs.get("azure_client_id")
    client_secret = kwargs.get("azure_client_secret")
    subscription_id = kwargs.get("subscription_id")
    
    if not all([tenant_id, client_id, client_secret, subscription_id]):
        raise ValueError("Credenciais do Azure não encontradas. Verifique se as variáveis do Airflow foram definidas.")

    client = ADFPipelineClient(subscription_id, tenant_id, client_id, client_secret)
    client.run_pipeline(resource_group, factory_name, pipeline_name, parameters, polling_interval)

# --- FUNÇÃO PARA EXECUTAR O JOB DO DATABRICKS ---
def trigger_databricks_job(**kwargs):    

    databricks_host = kwargs.get("databricks_host")
    databricks_token = kwargs.get("databricks_token")

    if not all([databricks_host, databricks_token]):
        raise ValueError("Credenciais do Databricks não encontradas. Verifique se as variáveis do Airflow foram definidas.")

    job_id = kwargs['job_id']
    app_reference = kwargs['app_reference']
    application = kwargs['application']
    date_partition = kwargs['date_partition']

    parameters = {
        "app_reference": app_reference,
        "application": application,
        "date_partition": date_partition,
        "layer_source": "raw",
    }

    run_url = f"{databricks_host}/api/2.1/jobs/run-now"
    run_status_url = f"{databricks_host}/api/2.1/jobs/runs/get"
    headers = {
        "Authorization": f"Bearer {databricks_token}",
        "Content-Type": "application/json"
    }

    try:
        logger.info(f"Iniciando o job Databricks com ID: {job_id} para {app_reference}")
        payload = {"job_id": job_id, "job_parameters": parameters}
        response = requests.post(run_url, headers=headers, json=payload)
        response.raise_for_status()
        run_id = response.json().get('run_id')
        logger.info(f"Job iniciado com sucesso. Run ID: {run_id}")

        while True:
            status_response = requests.get(run_status_url, headers=headers, params={"run_id": run_id})
            status_response.raise_for_status()
            status = status_response.json()
            life_cycle_state = status['state']['life_cycle_state']
            result_state = status['state'].get('result_state')
            logger.info(f"Status atual do job: {life_cycle_state}, resultado: {result_state}")

            if life_cycle_state in ['TERMINATED', 'SKIPPED', 'INTERNAL_ERROR']:
                if result_state != 'SUCCESS':
                    raise Exception(f"Job falhou ou foi cancelado. Detalhes: {json.dumps(status, indent=2)}")
                break
            time.sleep(10)
        logger.info(f"Job Databricks {job_id} finalizado com sucesso para {app_reference}!")
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro na requisição para a API do Databricks: {e}")
        if e.response is not None:
            logger.error(f"Detalhes do erro da API: {e.response.text}")
        raise
    except Exception as e:
        logger.error(f"Erro inesperado durante a execução do job: {e}")
        raise

# --- DEFINIÇÃO DA DAG ---
with DAG(
    dag_id="DAG_COMPASS_PIPELINE",
    default_args={
        "owner": "gacarvalho",
        "depends_on_past": False,
        "retries": 1,
        "email_on_failure": True,
        "email": ["gacarvalho.contato@gmail.com"],
    },
    description="Pipeline unificado e refatorado para ingestão e processamento de reviews.",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2024, 11, 29, tz="UTC"),
    catchup=False,
    tags=["compass", "adf", "databricks", "reviews", "refactor"],
) as dag:
    
    dm_init = DummyOperator(task_id="dm_init")

    # Centralize a configuração dos aplicativos aqui
    apps_config = [
        {"app_name": "bradesco", "app_id": "336954985"},
        {"app_name": "santander_way", "app_id": "1154266372"},
        {"app_name": "santander_br", "app_id": "613365711"},
        {"app_name": "nubank", "app_id": "814456780"},
        {"app_name": "itau", "app_id": "474505665"}
    ]

    # Parâmetros comuns para as tarefas do ADF
    adf_params = {
        "resource_group": "rg-data-compass",
        "factory_name": "datafact-compass",
        "polling_interval": 30,
    }

    # --- Grupo de tarefas: INGESTÃO DE REVIEWS (ADF) ---
    with TaskGroup("group_ingestion_adf", tooltip="Ingestão de Reviews para a RAW via ADF") as group_ingestion_adf:
        ingestion_tasks = {}
        for app in apps_config:
            # Cria a tarefa de ingestão dinamicamente para cada aplicativo
            task_id = f"ingest_adf_{app['app_name']}"
            ingestion_tasks[app['app_name']] = PythonOperator(
                task_id=task_id,
                python_callable=trigger_adf_pipeline,
                op_kwargs={
                    "pipeline_name": "pipeline_itunes_reviews",
                    "azure_tenant_id": Variable.get("AZURE_TENANT_ID"),
                    "azure_client_id": Variable.get("AZURE_CLIENT_ID"),
                    "azure_client_secret": Variable.get("AZURE_CLIENT_SECRET"),
                    "subscription_id": Variable.get("SUBSCRIPTION_ID"),
                    **adf_params,
                    "parameters": {
                        "appId": app['app_id'],
                        "appName": app['app_name'],
                    }
                }
            )

    # --- Grupo de tarefas: PROCESSAMENTO (DATABRICKS) ---
    with TaskGroup("group_processing_databricks", tooltip="Processamento de Reviews para a camada Bronze") as group_processing_databricks:
        processing_tasks = {}
        for app in apps_config:
            # Cria a tarefa de processamento dinamicamente para cada aplicativo
            task_id = f"process_databricks_{app['app_name']}"
            processing_tasks[app['app_name']] = PythonOperator(
                task_id=task_id,
                python_callable=trigger_databricks_job,
                op_kwargs={
                    "job_id": 549855026452349,
                    "app_reference": app['app_name'],
                    "application": "apple_reviews",
                    "layer_source": "raw",
                    "date_partition": "{{ ds }}",
                    "databricks_host": Variable.get("DATABRICKS_HOST"),
                    "databricks_token": Variable.get("DATABRICKS_TOKEN"),
                }
            )

    # --- DEFINIÇÃO DAS DEPENDÊNCIAS ---
    # Início da DAG -> Grupo de Ingestão
    dm_init >> group_ingestion_adf

    # O grupo de Ingestão deve ser concluído antes do grupo de Processamento
    group_ingestion_adf >> group_processing_databricks
