import time
import logging
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from scripts.load_data_from_mongo import insert_fake_feedbacks

# Configura o logger para a DAG
logger = logging.getLogger(__name__)

# --- DEFINIÇÃO DA DAG ---
with DAG(
    dag_id="DAG_E_COMPASS_LOAD_EVENTS_MONGODB",
    default_args={
        "owner": "gacarvalho",
        "depends_on_past": False,
        "retries": 1,
        "email_on_failure": False,  # desabilita enquanto testa
        "email": ["gacarvalho.contato@gmail.com"],
    },
    description="Pipeline para inserção de feedbacks fake no MongoDB.",
    schedule_interval=None,  # pode ser alterado para "0 0 * * *" depois
    start_date=pendulum.datetime(2024, 11, 29, tz="UTC"),
    catchup=False,
    tags=["compass", "reviews", "fake_data"],
) as dag:
    
    # Dummy init
    dm_init = DummyOperator(task_id="dm_init")

    # TaskGroup para gerar feedbacks fake
    with TaskGroup("group_generate_fake_feedbacks", tooltip="Inserção de feedbacks fake no MongoDB") as group_generate_fake_feedbacks:

        generate_fake_feedbacks = PythonOperator(
            task_id="generate_fake_feedbacks",
            python_callable=insert_fake_feedbacks,
            op_kwargs={
                "mongo_uri": Variable.get("MONGO_URI"),
                "db_name": "compass",
                "collection_name": "reviews_instituicao_compass",
                "num_feedbacks": 689
            }
        )


    # --- DEPENDÊNCIAS ---
    dm_init >> generate_fake_feedbacks
