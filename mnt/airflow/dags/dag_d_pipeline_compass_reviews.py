import time
import logging
import os
import pendulum
import requests
import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.storage.blob import BlobServiceClient
from airflow.models import Variable
from scripts.extract_mongo_data import extract_data_from_mongo

# Configura o logger para a DAG
logger = logging.getLogger(__name__)

# --- CLASSE DEDICADA PARA O AZURE DATA FACTORY ---
class ADFPipelineClient:
    """
    Cliente para interagir com o Azure Data Factory.
    Centraliza a lógica de autenticação e execução de pipelines.
    Agora usa a credencial padrão para suportar Managed Identity.
    """
    def __init__(self, subscription_id):
        self.credential = ClientSecretCredential(
            tenant_id=Variable.get("AZURE_TENANT_ID"),
            client_id=Variable.get("AZURE_CLIENT_ID"),
            client_secret=Variable.get("AZURE_CLIENT_SECRET")
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
        
    subscription_id = kwargs.get("subscription_id")
        
    if not subscription_id:
        raise ValueError("ID da assinatura do Azure não encontrada.")
    client = ADFPipelineClient(subscription_id)
    client.run_pipeline(resource_group, factory_name, pipeline_name, parameters, polling_interval)

# --- FUNÇÃO PARA TRANSFERIR O ARQUIVO PARA O BLOB COM AUTENTICAÇÃO VIA CLIENT SECRET ---
def upload_to_blob_with_token(**kwargs):
    ti = kwargs['ti']
    file_path = ti.xcom_pull(task_ids='group_raw_ingestion.group_raw_extract_internal_db.extract_from_mongodb', key='extracted_file_path')

    if not file_path or not os.path.exists(file_path):
        raise FileNotFoundError(f"Arquivo não encontrado em: {file_path}")
    now_str = datetime.utcnow().strftime("%Y-%m-%d_%H%M%S")
    container_name = "sa-compasslake"
    blob_name = f"raw_compass/internal_db/reviews/{kwargs['ds']}/reviews_mongo_{now_str}.json"
    account_url = Variable.get("AZURE_STORAGE_ACCOUNT_URL")
    
    # AUTENTICAÇÃO EXPLICITA COM CLIENT_SECRET
    credential = ClientSecretCredential(
        tenant_id=Variable.get("AZURE_TENANT_ID"),
        client_id=Variable.get("AZURE_CLIENT_ID"),
        client_secret=Variable.get("AZURE_CLIENT_SECRET")
    )
    try:
        blob_service_client = BlobServiceClient(account_url, credential=credential)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
                
        logger.info(f"Iniciando o upload do arquivo '{file_path}' para o Blob Storage...")
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
                    
        logger.info(f"Upload para '{blob_name}' no container '{container_name}' concluído com sucesso.")
            
    except Exception as e:
        logger.error(f"Erro durante o upload para o Azure Blob Storage: {e}")
        raise

# --- FUNÇÃO PARA EXECUTAR O JOB DO DATABRICKS ---
def trigger_databricks_job(**kwargs):
    databricks_host = kwargs.get("databricks_host")
    databricks_token = kwargs.get("databricks_token")
    if not all([databricks_host, databricks_token]):
        raise ValueError("Credenciais do Databricks não encontradas. Verifique se as variáveis do Airflow foram definidas.")
    
    job_id = kwargs['job_id']
    
    # PARÂMETROS OBRIGATÓRIOS E OPCIONAIS
    date_partition = kwargs['date_partition']
    application = kwargs.get('application')
    layer_source = kwargs.get('layer_source')
    app_reference = kwargs.get('app_reference')
    env = kwargs.get('env') # Novo parâmetro opcional
        
    parameters = {}
    
    # Constrói o dicionário de parâmetros apenas com os valores que existem
    if application:
        parameters["application"] = application
    if date_partition:
        parameters["date_partition"] = date_partition
    if layer_source:
        parameters["layer_source"] = layer_source
    if app_reference:
        parameters["app_reference"] = app_reference
    if env: # Adiciona o novo parâmetro se ele existir
        parameters["env"] = env

    run_url = f"{databricks_host}/api/2.1/jobs/run-now"
    run_status_url = f"{databricks_host}/api/2.1/jobs/runs/get"
    headers = {
        "Authorization": f"Bearer {databricks_token}",
        "Content-Type": "application/json"
    }

    try:
        log_message = f"Iniciando o job Databricks com ID: {job_id}."
        if application:
            log_message += f" para {application}"
        if layer_source:
            log_message += f" na camada {layer_source}"
        if app_reference:
            log_message += f" e app_reference: {app_reference}"
        if env:
            log_message += f" (ambiente: {env})"
        logger.info(log_message)
        
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
        
        logger.info(f"Job Databricks {job_id} finalizado com sucesso!")
        
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
    schedule_interval=None,
    start_date=pendulum.datetime(2024, 11, 29, tz="UTC"),
    catchup=False,
    tags=["compass", "adf", "databricks", "reviews", "refactor"],
) as dag:
        
    dm_init = DummyOperator(task_id="dm_init")

    # Configuração dos aplicativos
    apps_config = [
        {"app_name": "bradesco", "app_id": "336954985"},
        {"app_name": "santander_way", "app_id": "1154266372"},
        {"app_name": "santander_br", "app_id": "613365711"},
        {"app_name": "nubank", "app_id": "814456780"},
        {"app_name": "itau", "app_id": "474505665"}
    ]

    # Parâmetros comuns do ADF
    adf_params = {
        "resource_group": "rg-data-compass",
        "factory_name": "datafact-compass",
        "polling_interval": 30,
    }

    # --- CAMADA RAW (INGESTÃO) ---
    with TaskGroup("group_raw_ingestion", tooltip="Ingestão de dados para a camada RAW") as group_raw_ingestion:
        with TaskGroup("group_ingestion_adf", tooltip="Ingestão de Reviews para a RAW via ADF") as group_ingestion_adf:
            ingestion_tasks = {}
            for app in apps_config:
                task_id = f"ingest_adf_{app['app_name']}"
                ingestion_tasks[app['app_name']] = PythonOperator(
                    task_id=task_id,
                    python_callable=trigger_adf_pipeline,
                    op_kwargs={
                        "pipeline_name": "pipeline_itunes_reviews",
                        "subscription_id": Variable.get("SUBSCRIPTION_ID"),
                        **adf_params,
                        "parameters": {
                            "appId": app['app_id'],
                            "appName": app['app_name'],
                        }
                    }
                )

        with TaskGroup("group_raw_extract_internal_db", tooltip="Extração de dados internos para o Azure Blob Storage") as group_raw_extract_internal_db:
            extract_from_mongodb = PythonOperator(
                task_id="extract_from_mongodb",
                python_callable=extract_data_from_mongo,
                op_kwargs={
                    "mongo_uri": Variable.get("MONGO_URI"),
                    "db_name": "compass",
                    "collection_name": "reviews_instituicao_compass"
                }
            )
            transfer_to_blob = PythonOperator(
                task_id='transfer_file_to_azure_blob',
                python_callable=upload_to_blob_with_token,
            )
            extract_from_mongodb >> transfer_to_blob

    # --- CAMADA BRONZE (PROCESSAMENTO INICIAL) ---
    with TaskGroup("group_bronze_processing", tooltip="Processamento da camada RAW para BRONZE") as group_bronze_processing:
        with TaskGroup("group_databricks_itunes", tooltip="Processamento de Reviews do iTunes para a camada Bronze") as group_databricks_itunes:
            processing_tasks = {}
            for app in apps_config:
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
                        "env": "pre", # Adicionado o novo parâmetro
                        "databricks_host": Variable.get("DATABRICKS_HOST"),
                        "databricks_token": Variable.get("DATABRICKS_TOKEN"),
                    },
                    retries=3,
                    execution_timeout=timedelta(minutes=60)
                )

        with TaskGroup("group_databricks_internaldb", tooltip="Processamento de Reviews do banco interno para a camada Bronze") as group_databricks_internaldb:
            process_internaldb = PythonOperator(
                task_id="process_internaldb",
                python_callable=trigger_databricks_job,
                op_kwargs={
                    "job_id": 549855026452349,
                    "app_reference": "",
                    "application": "internal_db",
                    "layer_source": "raw",
                    "date_partition": "{{ ds }}",
                    "env": "pre", # Adicionado o novo parâmetro
                    "databricks_host": Variable.get("DATABRICKS_HOST"),
                    "databricks_token": Variable.get("DATABRICKS_TOKEN"),
                },
                retries=3,
                execution_timeout=timedelta(minutes=60)
            )

    # --- CAMADA SILVER (REFINAMENTO) ---
    with TaskGroup("group_databricks_silver", tooltip="Processamento de Reviews para a camada Silver") as group_databricks_silver:
        task_id = "process_databricks_silver"
        process_databricks_silver = PythonOperator(
            task_id=task_id,
            python_callable=trigger_databricks_job,
            op_kwargs={
                "job_id": 279987160157338,
                "application": "instituicao_reviews",
                "layer_source": "s_compass",
                "date_partition": "{{ ds }}",
                "env": "pre", # Adicionado o novo parâmetro
                "databricks_host": Variable.get("DATABRICKS_HOST"),
                "databricks_token": Variable.get("DATABRICKS_TOKEN"),
            },
            retries=3,
            execution_timeout=timedelta(minutes=60)
        )

    # --- CAMADA GOLD (AGREGACAO) ---
    with TaskGroup("group_databricks_gold", tooltip="Processamento de Reviews para a camada Gold") as group_databricks_gold:
        task_id = "process_databricks_gold"
        process_databricks_gold = PythonOperator(
            task_id=task_id,
            python_callable=trigger_databricks_job,
            op_kwargs={
                "job_id": 190064442350392,
                "date_partition": "{{ ds }}", 
                "env": "pre", # Adicionado o novo parâmetro
                "databricks_host": Variable.get("DATABRICKS_HOST"),
                "databricks_token": Variable.get("DATABRICKS_TOKEN"),
            },
            retries=3,
            execution_timeout=timedelta(minutes=60)
        )

    # --- DEPENDÊNCIAS ---
    dm_init >> group_raw_ingestion
    group_raw_ingestion >> group_bronze_processing
    group_bronze_processing >> group_databricks_silver
    group_databricks_silver >> group_databricks_gold
