# Databricks notebook source
# Módulos da biblioteca padrão do Python
import json
import logging
import re
import time
import warnings
from datetime import date, datetime, timedelta
from string import Template
from typing import Any, Dict, List, Tuple
import pyspark
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from datetime import datetime
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    coalesce, col, concat_ws, count, current_date, current_timestamp,
    date_format, date_sub, length, lit, regexp_replace, sha2,
    to_timestamp, trim, udf, upper, when
)
from pyspark.sql.types import (
    DataType, DecimalType, IntegerType, StringType, StructField,
    StructType, TimestampType
)

# Configuração de logging e ambiente
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logging.getLogger("pyspark").setLevel(logging.WARNING)
logger = logging.getLogger("compass.ingest")

# COMMAND ----------

# Recupera o SAS Token do secret scope
secret_scope_name = "storage_data"
storage_account_name = "compassdataprod"

# Recupera os secrets corretamente do scope
idLoganalytics = dbutils.secrets.get(scope=secret_scope_name, key="customeridLoganalytics")
keyLoganalytics = dbutils.secrets.get(scope=secret_scope_name, key="keyLoganalytics")
sas_token = dbutils.secrets.get(scope=secret_scope_name, key="adlsstoragekeydata")

# Configuração do Spark para o ADLS e otimizações do Delta Lake
spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account_name}.dfs.core.windows.net",
                "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account_name}.dfs.core.windows.net", sas_token)

# Otimizações do Delta Lake para ingestão
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

# COMMAND ----------

# Função utilitária
def get_config_compass(cfg: Row) -> tuple:
    """Extrai as configurações de uma linha da tabela de controle."""
    cfg_dict = cfg.asDict(recursive=True)

    # Blocos de configuração
    source_config   = cfg_dict.get("source_config", {}) or {}
    target_config   = cfg_dict.get("target_config", {}) or {}
    fallback_config = cfg_dict.get("fallback_config", {}) or {}

    # Regras de validação
    rule_control = {r.get("rule"): r.get("value") for r in cfg_dict.get("rule_control", [])}

    # Campos principais
    table_name_target = cfg_dict.get("table_name_target")
    schema_expected   = cfg_dict.get("schema_expected")
    schema_target     = cfg_dict.get("schema_target")
    schema_depara     = cfg_dict.get("schema_depara")
    version           = cfg_dict.get("version")
    last_update       = cfg_dict.get("last_modified")

    # Source configs
    source_format     = source_config.get("format")

    # Fallback configs
    create_empty_if_missing = fallback_config.get("create_empty_if_missing")

    # Target configs
    target_mode      = target_config.get("mode")
    target_directory = target_config.get("directory")
    target_format    = target_config.get("format")
    partitionBy      = target_config.get("partitionBy")

    logging.info("=== CONFIGURAÇÕES CARREGADAS ===")
    logging.info(f"Tabela de destino: {table_name_target}")
    logging.info(f"Regras de validação: {rule_control}")

    return (
        source_format, schema_expected, create_empty_if_missing, table_name_target,
        target_mode, target_directory, target_format, schema_target,
        schema_depara, partitionBy, rule_control, version, last_update
    )


# Parâmetros de entrada (vindos de widgets no Databricks)
date_partition    =   dbutils.widgets.get("date_partition") 
env               =   dbutils.widgets.get("env") 
table_target_name =   dbutils.widgets.get("table_target_name")
layer_target      =   dbutils.widgets.get("layer_target") 
layer_source      =   dbutils.widgets.get("layer_source")
source_table      = dbutils.widgets.get("source_table_name") 
source_table_name = f"{layer_source}.{source_table}"
table_target_name_full = f"{layer_target}.{table_target_name}"

params = {"date_partition": date_partition, "env": env, "table_target_name": table_target_name, "layer_target": layer_target, "layer_source": layer_source, "source_table_name": source_table}
logging.info("Parâmetros de entrada: %s", json.dumps(params))


# Fonte da tabela de controle
data_control = "metadata_compass.data_params"

try:
    compass_config = (
        spark.read.table(data_control)
        .filter(
            (F.col("source_layer") == layer_target)
            & (F.col("table_name_target") == table_target_name)
        )
        .orderBy(F.desc("version"))
        .take(1)
    )

    if not compass_config:
        raise ValueError(f"Nenhuma configuração encontrada para layer={layer_target} e tabela={table_target_name}")

except AnalysisException as e:
    logger.error(f"Falha ao ler a tabela de controle '{data_control}'. Erro: {e}")
    dbutils.notebook.exit("Falha na leitura da tabela de controle.")


# Extração da configuração
if compass_config:
    cfg = compass_config[0]

    (
        source_format, schema_expected, create_empty_if_missing, table_name_target,
        target_mode, target_directory, target_format, schema_target,
        schema_depara, partitionBy, rule_control, version, last_update
    ) = get_config_compass(cfg)

    # Montagem das variáveis derivadas
    table_target = f"{target_directory}.{table_name_target}"

    logging.info("Tabela final de destino: %s", table_target)
    logging.info("Target mode: %s | Target format: %s", target_mode, target_format)

# COMMAND ----------

# A classe ExecutionMetricsCollector não realiza nenhuma operação pesada no cluster. 
# Como? => Seu único papel é consolidar metadados, como o tempo de execução, e registrar os resultados das ações do Spark que já foram executadas. As operações que demandam maior processamento — como extração, padronização, carga e validação de dados — ocorrem de forma sequencial, disparando seus próprios jobs no cluster. Entre essas operações, a função validate_data() é a que mais impacta a performance, pois envolve múltiplas contagens e filtragens de registros que é executada => após a carga dos dados na tabela Bronze.
class ExecutionMetricsCollector:
    """Coleta métricas de execução, desvinculada do processamento de dados."""
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.start_time = None
        self.end_time = None

    def start_collection(self):
        """Marca o início do processo de coleta."""
        self.start_time = datetime.now()

    def end_collection(self):
        """Marca o fim do processo de coleta."""
        self.end_time = datetime.now()

    def collect_metrics(self, validation_results: dict, owner_data: dict, id_app: str) -> dict:
        """
        Gera um dicionário de métricas de execução a partir dos resultados de validação.
        """
        if not self.start_time or not self.end_time:
            raise ValueError("start_collection() e end_collection() precisam ser chamados.")
        
        # Cria as chaves ausentes com base no status do pipeline
        valid_count = validation_results["total_records"] if validation_results["status"] == "success" else 0
        invalid_count = validation_results["total_records"] if validation_results["status"] != "success" else 0
        
        valid_data_summary = {"count": valid_count, "percentage": (valid_count / validation_results["total_records"]) * 100 if validation_results["total_records"] > 0 else 0}
        invalid_data_summary = {"count": invalid_count, "percentage": (invalid_count / validation_results["total_records"]) * 100 if validation_results["total_records"] > 0 else 0}
        total_records = validation_results["total_records"]
        
        telemetry_validation_results = {
            "main_check": {
                "status": validation_results["status"] == "success",
                "message": validation_results["message"]
            }
        }
        
        total_time = (self.end_time - self.start_time).total_seconds()
        formatted_time = f"{total_time:.2f} s"

        metrics = {
            "owner": owner_data,
            "valid_data": valid_data_summary,
            "invalid_data": invalid_data_summary,
            "total_records": total_records,
            "total_processing_time": formatted_time,
            "validation_results": telemetry_validation_results, # Usa a nova estrutura
            "success_count": 1 if telemetry_validation_results["main_check"]["status"] else 0,
            "error_count": 0 if telemetry_validation_results["main_check"]["status"] else 1,
            "_ts": {
                "compass_start_ts": self.start_time.strftime("%Y-%m-%d %H:%M:%S"),
                "compass_end_ts": self.end_time.strftime("%Y-%m-%d %H:%M:%S"),
            },
            "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "app_id": id_app,
        }
        
        return metrics
    

def send_to_log_analytics(log_data: dict, log_type: str):
    """Envia um log JSON para o Azure Log Analytics."""
    customer_id = idLoganalytics
    shared_key = keyLoganalytics

    if not customer_id or not shared_key:
        logger.error("Credenciais para o Log Analytics não foram fornecidas. O log não será enviado.")
        return

    try:
        import requests, base64, hmac, hashlib
        from datetime import datetime
        
        body = json.dumps(log_data)
        rfc1123date = datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        content_length = len(body.encode('utf-8'))
        string_to_sign = f"POST\n{content_length}\napplication/json\nx-ms-date:{rfc1123date}\n/api/logs"
        bytes_to_sign = string_to_sign.encode('utf-8')
        encoded_key = base64.b64decode(shared_key)
        signed_string = base64.b64encode(hmac.new(encoded_key, bytes_to_sign, digestmod=hashlib.sha256).digest()).decode()
        signature = f"SharedKey {customer_id}:{signed_string}"
        
        headers = {
            'content-type': 'application/json',
            'Authorization': signature,
            'Log-Type': log_type,
            'x-ms-date': rfc1123date
        }
        
        url = f"https://{customer_id}.ods.opinsights.azure.com/api/logs?api-version=2016-04-01"
        response = requests.post(url, data=body.encode('utf-8'), headers=headers, timeout=10)
        
        if response.status_code >= 200 and response.status_code < 300:
            logger.info(f"Log de tipo '{log_type}' enviado com sucesso! Status: {response.status_code}")
        else:
            logger.error(f"Falha ao enviar o log. Status: {response.status_code}, Resposta: {response.text}")

    except requests.exceptions.Timeout:
        logger.error("Tempo de espera da requisição para o Log Analytics excedido.")
    except Exception as e:
        logger.error(f"Ocorreu uma exceção inesperada ao enviar o log. Erro: {e}")


# COMMAND ----------

collector = ExecutionMetricsCollector(spark)
collector.start_collection()

# Funções do pipeline de transformação
def func_read_data(
    spark: SparkSession,
    table_name: str,
    date_partition: str,
) -> DataFrame:
    """Lê dados de uma tabela Delta com filtro de partição otimizado."""
    try:
        yyyymm_ref = date_partition[:7].replace("-", "")
        logger.info(f"Lendo tabela '{table_name}' filtrando por 'date_load' == '{yyyymm_ref}'")

        df = spark.read.table(table_name)
        current_columns = df.columns

        if "date_load" not in current_columns:
            raise ValueError(f"Tabela '{table_name}' não contém a coluna 'date_load'.")

        df_filtered = df.filter(F.col("date_load") == yyyymm_ref)
        
        logger.info(f"Tabela '{table_name}' lida com sucesso. Total de {df_filtered.count()} registros.")
        return df_filtered

    except AnalysisException as e:
        logger.error(f"Erro ao ler a tabela '{table_name}': {e}")
        raise

def prepare_data(df: DataFrame) -> DataFrame:
    """
    Seleciona, trata e padroniza colunas em um DataFrame do Spark.
    """
    df_prepared = (
        df.select(
            'review_date',
            'review_rating',
            'review_version',
            'segment',
            'service_type',
            'app_reference'
        )
        .withColumn(
            'review_date',
            F.to_timestamp(F.col('review_date'), 'yyyy-MM-dd\'T\'HH:mm:ss.SSSZ')
        )
        .withColumn(
            'review_year',
            F.date_format(F.col('review_date'), 'yyyy')
        )
        .withColumn(
            'review_month',
            F.date_format(F.col('review_date'), 'MM')
        )
    )

    df_prepared = df_prepared.na.drop(subset=['review_date', 'review_rating'])
    
    return df_prepared

def validate_data(spark: SparkSession, df: DataFrame, compass_config: list, primary_key: list = None) -> dict:
    if primary_key is None: primary_key = []
    
    rule_list = [r.asDict() for r in compass_config[0]["rule_control"]]
    
    # ======== 1. Duplicidade ========
    if primary_key and all(c in df.columns for c in primary_key):
        df = df.withColumn("is_duplicate", (F.count("*").over(Window.partitionBy(*primary_key)) > 1))
    else:
        df = df.withColumn("is_duplicate", F.lit(False))

    # ======== 2. Nulos ========
    null_cols = [r.get("column_name") for r in rule_list if r.get("rule") == "not_empty" and r.get("value", "").lower() == "true"]
    from functools import reduce
    conditions = [F.col(c).isNull() | (F.trim(F.col(c)) == "") for c in null_cols if c in df.columns]
    df = df.withColumn("is_null_issue", reduce(lambda x, y: x | y, conditions, F.lit(False)))

    # ======== 3. Consistência de tipos ========
    df = df.withColumn("is_type_issue", F.lit(False))

    # ======== 4. Resumo em única passada ========
    summary = df.agg(
        F.count("*").alias("total_records"),
        F.sum(F.col("is_duplicate").cast("int")).alias("duplicate_count"),
        F.sum(F.col("is_null_issue").cast("int")).alias("null_issue_count"),
        F.sum(F.col("is_type_issue").cast("int")).alias("type_issue_count")
    ).collect()[0].asDict()

    total = summary["total_records"]
    valid = total - summary["duplicate_count"] - summary["null_issue_count"] - summary["type_issue_count"]
    invalid = total - valid

    return {
        "total_records": total,
        "valid_data": {"count": valid, "percentage": (valid / total) * 100 if total else 0},
        "invalid_data": {"count": invalid, "percentage": (invalid / total) * 100 if total else 0},
        "duplicate_check": {"status": summary["duplicate_count"] == 0, "count": summary["duplicate_count"]},
        "null_check": {"status": summary["null_issue_count"] == 0, "count": summary["null_issue_count"]},
        "type_consistency_check": {"status": summary["type_issue_count"] == 0, "count": summary["type_issue_count"]}
    }


def aggregate_reviews(df: DataFrame) -> DataFrame:
    """
    Agrega reviews por data, nota, versão, segmento, tipo de serviço e app,
    calculando a contagem de reviews e a média das avaliações por grupo.
    """
    df_aggregated = (
        df.groupBy(
            'review_year',
            'review_month',
            'review_version',
            'segment',
            'service_type',
            'app_reference'
        )
        .agg(
            F.count('*').alias('review_count'),
            F.round(F.avg('review_rating'), 1).cast('double').alias('average_rating'),
            F.min('review_rating').cast('double').alias('min_rating'),
            F.max('review_rating').cast('double').alias('max_rating'),
            F.round(
                (
                    (F.sum(F.when(F.col('review_rating') == 5, 1).otherwise(0)))
                    -
                    (F.sum(F.when(F.col('review_rating') <= 3, 1).otherwise(0)))
                )
                / F.count('*'),
                2
            ).cast('double').alias('nps_score')
        )
    )

    return df_aggregated
    
def save_data(
    df: DataFrame,
    table_name: str,
    target_mode: str,
    target_format: str
):
    """
    Grava um DataFrame como tabela Delta.
    """
    logger.info(f"Iniciando gravação da tabela '{table_name}' com modo '{target_mode}'.")
    
    if df.rdd.isEmpty():
        logger.warning("DataFrame vazio. Nenhuma gravação será realizada.")
        return

    writer = df.write.format(target_format).mode(target_mode)
    
    writer.option("overwriteSchema", "true").saveAsTable(table_name)
    logger.info(f"Tabela '{table_name}' gravada com sucesso.")


def optimize_delta_table(table_name: str, partition_col: str):


    logger.info(f"Iniciando otimização da tabela {table_name} para a partição {partition_col}.")
    optimize_cmd = f"OPTIMIZE {table_name} ZORDER BY {partition_col}"

    spark.sql(optimize_cmd)
    logger.info("Otimização concluída.")

# ==== Função principal de orquestração do pipeline ====
def run_pipeline(
    spark: SparkSession,
    date_partition: str,
    application: str,
    layer_source: str,
    env: str,
    source_table_name: str,
    table_target_name: str,
    target_mode: str,
    target_format: str,
    idLoganalytics: str,
    keyLoganalytics: str
):
    try:
        # 1. Leitura de Dados
        df = func_read_data(
            spark,
            table_name=source_table_name,
            date_partition=date_partition,
        )

        # 2. Processamento e Transformação
        prepared_df = prepare_data(df)
        aggregated_df = aggregate_reviews(prepared_df)

        # 3. Validação de Qualidade de Dados (Mock)
        total_records_out = aggregated_df.count()
        validation_results = {
            "total_records": total_records_out,
            "status": "success" if total_records_out > 0 else "warning",
            "message": "Nenhum registro a ser gravado." if total_records_out == 0 else "Dados agregados com sucesso."
        }
        
        if env =="pre":
                aggregated_df.printSchema()

        # 4. Escrita da Tabela Delta
        save_data(
            df=aggregated_df, 
            table_name=table_target_name, 
            target_mode=target_mode,
            target_format=target_format
        )


        if env == "pre":
            display(aggregated_df)

        optimize_delta_table(table_name=table_target_name, partition_col="review_month")

        # 5. Telemetria e Logs Finais
        collector.end_collection()
        owner_data = {
            "dominio": "DOMINIO_INSTITUICAO",
            "projeto": "compass",
            "layer_lake": "gold",
            "layer_source": layer_source
        }
        final_metrics = collector.collect_metrics(
            validation_results=validation_results,
            owner_data=owner_data,
            id_app=application
        )

        if env == "pre":
            final_json_log = json.dumps(final_metrics, indent=2)
            logger.info("Métricas finais de execução:\n" + final_json_log)

        send_to_log_analytics(log_data=final_metrics, log_type="CompassLogs_CL")
        logger.info("Processo de agregação e monitoramento finalizado com sucesso.")

    except Exception as e:
        logger.error(f"Falha crítica no pipeline. Erro: {e}", exc_info=True)
        dbutils.notebook.exit(f"Falha no pipeline: {e}")

# ==== Chamada final do pipeline ====
run_pipeline(
    spark=spark,
    date_partition=date_partition,
    application=table_target_name,
    layer_source=layer_source,
    env=env,
    source_table_name=source_table_name,
    table_target_name=table_target_name_full,
    target_mode=target_mode,
    target_format=target_format,
    idLoganalytics=idLoganalytics,
    keyLoganalytics=keyLoganalytics
)