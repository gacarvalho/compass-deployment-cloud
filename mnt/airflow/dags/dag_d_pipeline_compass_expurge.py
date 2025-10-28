import time
import logging
import pendulum
import requests
import json
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

logger = logging.getLogger(__name__)

# -----------------------------
# FUNÇÃO DE EXECUÇÃO DO JOB DATABRICKS
# -----------------------------
def trigger_databricks_expurge(**kwargs):
    """
    Executa o job Databricks responsável por expurgar dados no ADLS (camada RAW).
    """
    databricks_host = Variable.get("DATABRICKS_HOST")
    databricks_token = Variable.get("DATABRICKS_TOKEN")
    job_id = "182057076138129"

    # Pega parâmetros vindos do TriggerDagRunOperator
    conf = kwargs.get("dag_run").conf or {}
    date_partition = conf.get("date_partition", kwargs.get("ds"))
    env = conf.get("env", "pre")

    if not date_partition:
        raise ValueError("O parâmetro 'date_partition' é obrigatório.")

    headers = {
        "Authorization": f"Bearer {databricks_token}",
        "Content-Type": "application/json"
    }

    payload = {
        "job_id": job_id,
        "job_parameters": {
            "date_partition": date_partition,
            "env": env
        }
    }

    run_url = f"{databricks_host}/api/2.1/jobs/run-now"
    run_status_url = f"{databricks_host}/api/2.1/jobs/runs/get"

    try:
        logger.info(f"🚀 Iniciando job Databricks de expurgo ({job_id}) com parâmetros: {payload}")
        response = requests.post(run_url, headers=headers, json=payload)
        response.raise_for_status()
        run_id = response.json().get("run_id")

        # Loop de monitoramento do status
        while True:
            status_response = requests.get(run_status_url, headers=headers, params={"run_id": run_id})
            status_response.raise_for_status()
            state = status_response.json()["state"]
            life_cycle_state = state["life_cycle_state"]
            result_state = state.get("result_state")

            logger.info(f"Status do expurgo: {life_cycle_state} | Resultado: {result_state}")

            if life_cycle_state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
                if result_state != "SUCCESS":
                    raise Exception(f"Job Databricks de expurgo falhou: {json.dumps(status_response.json(), indent=2)}")
                logger.info("Job Databricks de expurgo finalizado com sucesso.")
                break

            time.sleep(10)

    except Exception as e:
        logger.error(f"Erro ao executar job Databricks de expurgo: {e}")
        raise

# -----------------------------
# DAG DE EXPURGO
# -----------------------------
with DAG(
    dag_id="DAG_COMPASS_PIPELINE_EXPURGE",
    description="Executa o job Databricks responsável pelo expurgo da camada RAW.",
    schedule_interval=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["databricks", "expurgo", "cleanup", "raw"],
    default_args={
        "depends_on_past": False,
        "email_on_failure": True,
        "retries": 0
    },
) as dag:

    expurge_databricks = PythonOperator(
        task_id="run_expurge_job",
        python_callable=trigger_databricks_expurge,
        provide_context=True,
    )

    expurge_databricks

