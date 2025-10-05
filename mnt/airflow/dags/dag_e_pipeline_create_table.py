import time
import logging
import requests
import json
import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task

# Configura o logger
logger = logging.getLogger("airflow.task")

# =========================
# Funções auxiliares
# =========================
def start_databricks_job(databricks_host: str, databricks_token: str, job_id: int) -> int:
    """
    Dispara um job no Databricks e retorna o run_id.
    """
    run_url = f"{databricks_host}/api/2.1/jobs/run-now"
    headers = {"Authorization": f"Bearer {databricks_token}", "Content-Type": "application/json"}

    payload = {"job_id": job_id}
    try:
        response = requests.post(run_url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        run_id = response.json().get("run_id")

        if not run_id:
            raise RuntimeError(f"Resposta inesperada da API: {response.text}")

        logger.info(f"Job {job_id} iniciado com Run ID {run_id}.")
        return run_id

    except requests.RequestException as e:
        logger.error(f"Erro ao iniciar job {job_id}: {e}")
        raise


def monitor_databricks_job(databricks_host: str, databricks_token: str, run_id: int, poll_interval: int = 15):
    """
    Monitora o job até a conclusão.
    """
    status_url = f"{databricks_host}/api/2.1/jobs/runs/get"
    headers = {"Authorization": f"Bearer {databricks_token}"}

    while True:
        try:
            response = requests.get(status_url, headers=headers, params={"run_id": run_id}, timeout=30)
            response.raise_for_status()
            status = response.json()

            life_cycle = status["state"]["life_cycle_state"]
            result_state = status["state"].get("result_state")

            logger.info(f"Run {run_id} - Estado atual: {life_cycle}, Resultado: {result_state}")

            if life_cycle in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
                if result_state != "SUCCESS":
                    raise RuntimeError(f"Job falhou ou foi cancelado. Detalhes: {json.dumps(status, indent=2)}")
                logger.info(f"Job {run_id} finalizado com sucesso.")
                break

        except requests.RequestException as e:
            logger.error(f"Erro ao monitorar run {run_id}: {e}")
            raise

        time.sleep(poll_interval)


# =========================
# TaskFlow API
# =========================
@task(retries=3, retry_delay=timedelta(minutes=2))
def run_databricks_job(job_id: int = 426749784260265):
    databricks_host = Variable.get("DATABRICKS_HOST")
    databricks_token = Variable.get("DATABRICKS_TOKEN")

    run_id = start_databricks_job(databricks_host, databricks_token, job_id)
    monitor_databricks_job(databricks_host, databricks_token, run_id)


# =========================
# DAG Definition
# =========================
with DAG(
    dag_id="DAG_E_CREATE_TABLE_COMPASS",
    default_args={"owner": "gacarvalho", "depends_on_past": False, "email_on_failure": False},
    description="Pipeline para criacao de tabela.",
    schedule_interval=None,
    start_date=pendulum.datetime(2024, 11, 29, tz="UTC"),
    catchup=False,
    tags=["compass", "databricks", "reviews", "governance"],
) as dag:

    process_itunes_reviews = run_databricks_job()
