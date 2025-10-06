# Databricks notebook source
# 1) IMPORTS & LOGGING
import json
import logging
import requests
import yaml
from string import Template
from datetime import datetime, date
from typing import List, Dict, Tuple
from requests.adapters import HTTPAdapter, Retry
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, lit, current_timestamp

# COMMAND ----------


logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("compass.apple")

storage_account_name = "compassdataprod"
secret_scope_name = "storage_data"
secret_key_name   = "adlsstoragekeydata"


container_bronze = "b-compass"
container_silver = "s-compass"
container_gold = "g-compass"
container_system = "system-compass"

# Recupera o SAS Token do secret scope
sas_token = dbutils.secrets.get(scope=secret_scope_name, key=secret_key_name)

# Configuração com SAS Token
spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account_name}.dfs.core.windows.net", sas_token)


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import current_timestamp


def create_delta_table_apple_reviews(delta_table_path, db_name, table_name):
    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        
        create_string = f"""
            CREATE TABLE IF NOT EXISTS {db_name}.{table_name}
                    (
                        author_ura              STRING      COMMENT 'URL do autor da avaliação',
                        author_name             STRING      COMMENT 'Nome do autor da avaliação',
                        author_label            STRING      COMMENT 'Label do autor, se existir',
                        updated_at              STRING      COMMENT 'Data de atualização do review',
                        rating                  INT         COMMENT 'Nota atribuída pelo usuário',
                        version                 STRING      COMMENT 'Versão do aplicativo avaliado',
                        review_id               STRING      COMMENT 'Identificador único da avaliação',
                        title                   STRING      COMMENT 'Título do review',
                        content                 STRING      COMMENT 'Conteúdo do review',
                        content_type_attribute  STRING      COMMENT 'Tipo do conteúdo (text, etc)',
                        link_rel                STRING      COMMENT 'Tipo do link relacionado',
                        link_href               STRING      COMMENT 'URL do link relacionado',
                        vote_sum                INT         COMMENT 'Soma dos votos',
                        content_term            STRING      COMMENT 'Termo do tipo de conteúdo',
                        content_label           STRING      COMMENT 'Label do tipo de conteúdo',
                        vote_count              INT         COMMENT 'Número de votos',
                        date_load               STRING      COMMENT 'Data da carga do dado em partição',
                        app_reference           STRING      COMMENT 'Referência do app',
                        ingestion_ts            TIMESTAMP   COMMENT 'Timestamp de ingestão'
                    )

                USING DELTA
                PARTITIONED BY (date_load)
                LOCATION '{delta_table_path}'
        """
        spark.sql(create_string)
        return logger.info(f"Tabela {table_name} criada com sucesso")
    
    except Exception as e:
        logger.error(f"Erro na criação da tabela: {e}")
        raise Exception(f"Critical error create table. Job terminated.")

table_name = "apple_reviews"
path_apple = f"abfss://{container_bronze}@{storage_account_name}.dfs.core.windows.net/b_compass/{table_name}"

create_delta_table_apple_reviews(path_apple, "b_compass", "apple_reviews")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

def create_delta_table_internal_reviewsl(delta_table_path, db_name, table_name):
    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        
        create_string = f"""
            CREATE TABLE IF NOT EXISTS {db_name}.{table_name}
            (
                submission_date             TIMESTAMP   COMMENT 'Data e hora do envio do feedback',
                client_segment              STRING      COMMENT 'Segmento do cliente (pf/pj)',
                client_identification       STRING      COMMENT 'Identificação do cliente (CPF ou CNPJ)',
                client_classification       STRING      COMMENT 'Classificação do cliente (small, medium, large)',
                feedback_rating             INT         COMMENT 'Nota atribuída pelo cliente',
                feedback_comment            STRING      COMMENT 'Comentário textual do cliente',
                service_type                STRING      COMMENT 'Tipo do serviço relacionado ao feedback',
                service_id                  STRING      COMMENT 'Identificador único do serviço',
                service_feedback            STRING      COMMENT 'Comentário específico sobre o serviço',
                source_channel              STRING      COMMENT 'Canal de origem do feedback (website, app, etc.)',
                source_id                   STRING      COMMENT 'Identificação da origem do feedback',
                app_reference               STRING      COMMENT 'Identificador do app origem',
                source_user_agent           STRING      COMMENT 'User agent do cliente que enviou o feedback',
                ingestion_ts                TIMESTAMP   COMMENT 'Timestamp de ingestão do dado',
                date_load                   STRING COMMENT 'Data da carga do dado em partição'
            )
            USING DELTA
            PARTITIONED BY (date_load)
            LOCATION '{delta_table_path}'
        """
        spark.sql(create_string)
        return logger.info(f"Tabela {table_name} criada com sucesso")
    
    except Exception as e:
        logger.error(f"Erro na criação da tabela: {e}")
        raise Exception(f"Critical error create table. Job terminated.")

# Exemplo de chamada
table_name = "internal_db"
path_control = f"abfss://{container_bronze}@{storage_account_name}.dfs.core.windows.net/b_compass/{table_name}"

create_delta_table_internal_reviewsl(path_control, "b_compass", table_name)


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import current_timestamp
# Assumindo que 'spark' e 'logger' estão definidos no seu ambiente

def create_delta_table_instituicao_reviews(delta_table_path, db_name, table_name, recreate: bool = False):
    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

        # 1. Lógica para "Recriar" (DROP IF EXISTS)
        if recreate:
            logger.warning(f"Recriando tabela: DROP TABLE IF EXISTS {db_name}.{table_name}")
            # O CASCADE garante que visões dependentes também sejam descartadas.
            spark.sql(f"DROP TABLE IF EXISTS {db_name}.{table_name}")
        
        # O CREATE TABLE IF NOT EXISTS garante que o código não falhe se o 'recreate' for False
        create_string = f"""
            CREATE TABLE IF NOT EXISTS {db_name}.{table_name}
                    (
                        review_id STRING COMMENT 'Chave única do registro de avaliação.',
                        client_id STRING COMMENT 'Identificador anonimizado do cliente.',
                        review_date TIMESTAMP COMMENT 'Data e hora da avaliação.',
                        review_rating INTEGER COMMENT 'Classificação da avaliação, de 1 a 5.',
                        review_title STRING COMMENT 'Título do feedback do cliente.',
                        review_text STRING COMMENT 'Texto completo da avaliação.',
                        review_version STRING COMMENT 'Versão do aplicativo.',
                        source_channel STRING COMMENT 'Canal de origem do feedback.',
                        source_system STRING COMMENT 'Sistema de onde o feedback foi originou.',
                        segment STRING COMMENT 'Segmento de negócio associado ao feedback.',
                        service_type STRING COMMENT 'Tipo de serviço relacionado ao feedback.',
                        source_agent STRING COMMENT 'Plataforma do feedback',
                        ingestion_ts TIMESTAMP COMMENT 'Timestamp da ingestão do registro.',
                        user_agent STRING COMMENT 'Agente de usuário do cliente.',
                        app_reference STRING COMMENT 'Referência única do aplicativo.'
                        )
                        USING DELTA
                        COMMENT 'Tabela unificada e limpa de avaliações na camada Silver.'
                        PARTITIONED BY (date_load STRING COMMENT 'Data de carregamento no formato YYYY-MM.')
                LOCATION '{delta_table_path}'
        """
        spark.sql(create_string)
        return logger.info(f"Tabela {table_name} criada/atualizada com sucesso.")
    
    except Exception as e:
        logger.error(f"Erro na criação da tabela: {e}")
        raise Exception(f"Critical error create table. Job terminated.")

# --- Execução da Recriação ---

table_name = "instituicao_reviews"
path_instituicao_reviews = f"abfss://{container_silver}@{storage_account_name}.dfs.core.windows.net/s_compass/{table_name}"

# CHAME A FUNÇÃO PASSANDO 'recreate=True'
create_delta_table_instituicao_reviews(
    delta_table_path=path_instituicao_reviews, 
    db_name="s_compass", 
    table_name="instituicao_reviews",
    recreate=False
)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import current_timestamp


def create_delta_table_control_params(delta_table_path, db_name, table_name):
    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        
        create_string = f"""
            CREATE TABLE IF NOT EXISTS {db_name}.{table_name}
            (
                version DOUBLE,
                source_layer STRING,
                table_name_target STRING,
                schema_expected ARRAY<
                    STRUCT<
                        name_column: STRING,
                        type_column: STRING,
                        other: STRING
                    >
                >,
                schema_target ARRAY<
                    STRUCT<
                        name_column: STRING,
                        type_column: STRING
                    >
                >,
                schema_depara ARRAY<
                    STRUCT<
                        source_column: STRING,
                        target_column: STRING
                    >
                >,
                rule_control ARRAY<
                    STRUCT<
                        rule: STRING,
                        value: STRING
                    >
                >,
                source_config MAP<STRING, STRING>,
                target_config MAP<STRING, STRING>,
                fallback_config MAP<STRING, STRING>,
                last_modified TIMESTAMP
            )
            USING DELTA
            PARTITIONED BY (version)
            LOCATION '{delta_table_path}'

        """
        spark.sql(create_string)
        return logger.info(f"Tabela {table_name} criada com sucesso")
    
    except Exception as e:
        logger.error(f"Erro na criação da tabela: {e}")
        raise Exception(f"Critical error create table. Job terminated.")

path_controle_data_params = f"abfss://{container_system}@{storage_account_name}.dfs.core.windows.net/metadata_compass/data_params/"

create_delta_table_control_params(path_controle_data_params, "metadata_compass", "data_params")



# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import current_timestamp

def create_delta_table_gold(delta_table_path, db_name, table_name):
    """
    Cria uma tabela Delta para armazenar dados de reviews de aplicativos.
    """
    try:
        # Garante que o banco de dados exista
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

        create_string = f"""
            CREATE TABLE IF NOT EXISTS {db_name}.{table_name}
            (
                review_year INTEGER COMMENT 'Ano da review',
                review_month INTEGER COMMENT 'Mês da review',
                review_version STRING COMMENT 'Versão do aplicativo analisada',
                segment STRING COMMENT 'Segmento de cliente (PF/PJ)',
                service_type STRING COMMENT 'Tipo de serviço (ex: EMPRESTIMO, CARTAO_DE_CREDITO)',
                app_reference STRING COMMENT 'Referência do aplicativo',
                review_count INTEGER COMMENT 'Número de reviews',
                average_rating DOUBLE COMMENT 'Avaliação média',
                min_rating INTEGER COMMENT 'Avaliação mínima',
                max_rating INTEGER COMMENT 'Avaliação máxima',
                nps_score INTEGER COMMENT 'Score NPS (Net Promoter Score)'
            )
            USING DELTA
            PARTITIONED BY (review_year)
            LOCATION '{delta_table_path}'
        """
        spark.sql(create_string)
        print(f"Tabela {table_name} criada com sucesso em {db_name}")
        return True

    except Exception as e:
        print(f"Erro na criação da tabela {table_name}: {e}")
        raise Exception(f"Erro crítico na criação da tabela. Job terminado.")

table = "reviews_customer_compass"
path_controle_reviews_customer_compass = f"abfss://{container_gold}@{storage_account_name}.dfs.core.windows.net/g_compass/{table}/"

create_delta_table_gold(path_controle_reviews_customer_compass, "g_compass", table)