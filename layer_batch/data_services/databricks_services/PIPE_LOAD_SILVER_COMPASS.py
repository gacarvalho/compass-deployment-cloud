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
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    coalesce, col, concat_ws, count, current_date, current_timestamp,
    date_format, date_sub, length, lit, regexp_replace, sha2,
    to_timestamp, trim, udf, upper, when, cast
)
from pyspark.sql.types import (
    DataType, DecimalType, IntegerType, StringType, StructField,
    StructType, TimestampType
)

# Configuração de logging e ambiente
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logging.getLogger("pyspark").setLevel(logging.WARNING)
logger = logging.getLogger("compass.silver")

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


# Mapeamento de tipos para reutilização
type_map = {
    "STRING": T.StringType(),
    "INT": T.IntegerType(),
    "INTEGER": T.IntegerType(),
    "LONG": T.LongType(),
    "BIGINT": T.LongType(),
    "DOUBLE": T.DoubleType(),
    "FLOAT": T.DoubleType(),
    "BOOLEAN": T.BooleanType(),
    "DATE": T.DateType(),
    "TIMESTAMP": T.TimestampType(),
}

# Parâmetros de entrada do job (widgets)
date_partition = dbutils.widgets.get("date_partition")
application = dbutils.widgets.get("application")
layer_source = dbutils.widgets.get("layer_source")
env = dbutils.widgets.get("env")


params = {
    "date_partition": date_partition,
    "application": application,
    "layer_source": layer_source
}

logging.info("Parâmetros de entrada: %s", json.dumps(params))

# Lê a configuração a partir da tabela de controle
try:
    data_control = "metadata_compass.data_params"
    compass_config = spark.read.table(data_control) \
        .filter((F.col("source_layer") == layer_source) & (F.col("table_name_target") == application)) \
        .orderBy(F.desc("version")).take(1)

    if not compass_config:
        raise ValueError(f"Nenhuma configuração encontrada para layer {layer_source} e aplicação {application}")

except AnalysisException as e:
    logger.error(f"Falha ao ler a tabela de controle '{data_control}'. Erro: {e}")
    dbutils.notebook.exit("Falha na leitura da tabela de controle.")

# Funções de Telemetria e Utilitários
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

        # Remove as chaves duplicadas do dicionário 'validation_results'
        valid_data_summary = validation_results.pop("valid_data")
        invalid_data_summary = validation_results.pop("invalid_data")

        total_records = validation_results["total_records"]
        total_time = (self.end_time - self.start_time).total_seconds()
        formatted_time = f"{total_time:.2f} s"
        
        # Define as chaves de validação a serem contadas
        validation_keys = ["duplicate_check", "null_check", "type_consistency_check"]
        
        success_count = sum(1 for key in validation_keys if validation_results.get(key, {}).get("status", False))
        error_count = sum(1 for key in validation_keys if not validation_results.get(key, {}).get("status", False))
        
        metrics = {
            "owner": owner_data,
            "valid_data": valid_data_summary,
            "invalid_data": invalid_data_summary,
            "total_records": total_records,
            "total_processing_time": formatted_time,
            "validation_results": validation_results,
            "success_count": success_count,
            "error_count": error_count,
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

collector = ExecutionMetricsCollector(spark)
collector.start_collection()


def get_config_compass(cfg: Row, layer_source: str) -> tuple:
    """Extrai as configurações de uma linha da tabela de controle."""
    cfg_dict = cfg.asDict(recursive=True)
    source_config = cfg_dict.get("source_config", {})
    target_config = cfg_dict.get("target_config", {})
    fallback_config = cfg_dict.get("fallback_config", {})
    
    rule_control = {r.get("rule"): r.get("value") for r in cfg_dict.get("rule_control", [])}

    table_name_target = cfg_dict.get("table_name_target")
    schema_expected = cfg_dict.get("schema_expected")
    schema_target = cfg_dict.get("schema_target")
    schema_depara = cfg_dict.get("schema_depara")
    version = cfg_dict.get("version")
    last_update = cfg_dict.get("last_modified")
    
    source_format = source_config.get("format")
    create_empty_if_missing = fallback_config.get("create_empty_if_missing")
    target_mode = target_config.get("mode")
    target_directory = target_config.get("directory")
    target_format = target_config.get("format")
    partitionBy = target_config.get("partitionBy")
    table_name_target_full = f"{layer_source}.{table_name_target}"


    logging.info("=== CONFIGURACOES CARREGADAS ===")
    logging.info(f"Tabela de destino: {table_name_target}")
    logging.info(f"Regras de validação: {rule_control}")

    return (
        source_format, schema_expected, create_empty_if_missing, table_name_target, table_name_target_full,
        target_mode, target_directory, target_format, schema_target,
        schema_depara, partitionBy, rule_control, version, last_update
    )



def validate_data(spark: SparkSession, df: DataFrame, compass_config=None, primary_key: list = None) -> dict:
    if primary_key is None: primary_key = []
    
    rule_list = [r.asDict() for r in compass_config["rule_control"]]
   
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
    
def func_read_data(
    spark: SparkSession,
    table_name: str,
    columns_to_select: list[str],
    days_load: str
) -> DataFrame:
    """Lê dados de uma tabela Delta com filtro de partição otimizado."""
    try:
        days_load_int = int(days_load)
        
        df = spark.read.table(table_name).filter(
            F.col("date_load") >= F.date_sub(F.current_date(), days_load_int)
        )
        logger.info(f"Tabela '{table_name}' lida com sucesso.")

        current_columns = df.columns
        missing_cols = [c for c in columns_to_select if c not in current_columns]

        if missing_cols:
            raise ValueError(f"Tabela '{table_name}' não contém as colunas obrigatórias: {missing_cols}")

        df = df.withColumn("source_table", lit(table_name))
        selected_cols = columns_to_select + ["source_table", "date_load"]

        return df.select(selected_cols)

    except AnalysisException as e:
        logger.error(f"Erro ao ler a tabela '{table_name}': {e}")
        raise

def unify_data(df_apple: DataFrame, df_internal: DataFrame) -> DataFrame:
    """Unifica e padroniza os DataFrames de reviews externos e internos, garantindo a conformidade
       e a qualidade dos dados para a camada Silver.
    """
    
    # Valores Padrão para Nulos/Vazios
    DEFAULT_STRING = F.lit("NAO_INFORMADO")
    DEFAULT_RATING = F.lit(0) # Padrão de 0 para ratings nulos/inválidos
    
    # ----------------------------------------------------------------------
    # 1. Tratamento e Seleção de Dados Apple
    # ----------------------------------------------------------------------
    df_apple_silver = (
        df_apple.withColumn("review_id", sha2(F.col("review_id"), 256))
        .select(
            F.col("review_id"),
            F.col("author_name").alias("client_id"),
            F.to_timestamp(F.col("updated_at"), 'yyyy-MM-dd\'T\'HH:mm:ssXXX').alias("review_date"),
            F.col("rating").alias("review_rating"),
            F.coalesce(F.col("title"), F.lit("NAO_IDENTIFICADO")).alias("review_title"),
            F.coalesce(F.col("content"), DEFAULT_STRING).alias("review_text"),
            F.coalesce(F.col("version"), F.lit("1.0.0")).alias("review_version"),
            F.lit("external").alias("source_channel"),
            F.lit("apple_reviews").alias("source_system"),
            F.lit("NA").alias("segment"),
            F.lit("NA").alias("service_type"),
            F.coalesce(F.lit("Itunes"), DEFAULT_STRING).alias("source_agent"),
            F.col("ingestion_ts"),
            F.col("date_load"),
            F.lit("REVIEWS").alias("user_agent"),
            F.upper(F.coalesce(F.col("app_reference"), DEFAULT_STRING)).alias("app_reference")
        )
    )
    
    # ----------------------------------------------------------------------
    # 2. Tratamento e Seleção de Dados Internos
    # ----------------------------------------------------------------------
    df_internal_silver = (
        df_internal.withColumn("review_id", sha2(concat_ws(":", "client_identification", "app_reference"), 256))
        .select(
            # Chaves e Datas
            F.col("review_id"),
            F.col("client_identification").alias("client_id"),
            F.to_timestamp(F.col("submission_date")).alias("review_date"),            
            F.coalesce(F.col("feedback_rating"), DEFAULT_RATING)
             .cast("integer").alias("review_rating"),            
            F.lit("NAO_IDENTIFICADO").alias("review_title"),
            F.when((F.coalesce(F.col("feedback_comment"), F.lit("")) == F.lit("")), DEFAULT_STRING)
                .otherwise(F.col("feedback_comment")).alias("review_text"),            
            F.coalesce(F.col("source_id").cast("string"), F.lit("1.0.0")).alias("review_version"),
            F.col("source_channel"),
            F.lit("internaldb_reviews").alias("source_system"),
            F.upper(F.coalesce(F.col("client_segment"), F.lit("NA"))).alias("segment"),
            F.coalesce(F.col("service_type"), F.lit("NA")).alias("service_type"),
            F.coalesce(F.col("source_user_agent"), DEFAULT_STRING).alias("source_agent"),
            F.col("ingestion_ts"),
            F.col("date_load"),
            F.lit("REVIEWS").alias("user_agent"),
            F.upper(F.coalesce(F.col("app_reference"), DEFAULT_STRING)).alias("app_reference")
        )
    )
    
    # ----------------------------------------------------------------------
    # 3. Unificação e Retorno
    # ----------------------------------------------------------------------
    df_unified = df_apple_silver.unionByName(df_internal_silver, allowMissingColumns=True)

    return df_unified



def apply_data_rules(df: DataFrame, rules: dict) -> DataFrame:
    """Aplica regras de negócio para filtragem e desduplicação."""
    df_filtered = df.dropDuplicates(["review_id", "client_id","source_system", "app_reference"])
    
    if rules.get("not_empty", "false").lower() == "true":
        df_filtered = df_filtered.filter(col("review_rating").isNotNull())

    min_val = int(rules.get("min_value", 1))
    max_val = int(rules.get("max_value", 5))
    df_filtered = df_filtered.filter(
        (col("review_rating") >= min_val) & (col("review_rating") <= max_val)
    )

    return df_filtered

def processing_reviews(df: DataFrame) -> DataFrame:
    """
    Normaliza e limpa campos de texto, removendo acentos e emojis/símbolos.
    """
    
    # 1. Definição do Mapa de Acentos
    original_chars = 'ÁÀÃÂÄÉÈÊËÍÌÎÏÓÒÕÔÖÚÙÛÜÇáàãâäéèêëíìîïóòõôöúùûüç'
    replacement_chars = 'AAAAAEEEEIIIIOOOOOUUUUCaaaaaeeeeiiiiiooooouuuuc'

    columns_to_process = [
        "review_text", "review_title", "source_channel", 
        "client_id", "service_type", "source_system"
    ]
    
    # Colunas que podem conter emojis
    columns_with_emojis = ["review_text", "review_title"]
    
    # PADRÃO CORRIGIDO E SEGURO: Remove tudo que NÃO é Letra, Número, Espaço ou Pontuação.
    # O Spark SQL interpreta \p{...} para classes Unicode.
    SAFE_EMOJI_AND_SYMBOL_PATTERN = r'[^\p{L}\p{N}\s\p{P}]' 

    df_clean = df

    # 2. Remoção de Acentos (Single Pass Translate)
    for col_name in columns_to_process:
        df_clean = df_clean.withColumn(
            col_name, 
            F.translate(F.col(col_name), original_chars, replacement_chars)
        )
        
    # 3. Remoção de Emojis/Símbolos (ETAPA CORRIGIDA)
    # Remove todos os caracteres que não se encaixam nas classes Unicode básicas (Letra/Número/Espaço/Pontuação).
    for col_name in columns_with_emojis:
        df_clean = df_clean.withColumn(
            col_name,
            F.regexp_replace(F.col(col_name), SAFE_EMOJI_AND_SYMBOL_PATTERN, "")
        )
        
    # 4. Padronização e Limpeza Final
    df_clean = (
        df_clean
        .withColumn("source_system", upper(trim("source_system")))
        .withColumn("review_text", upper(trim("review_text")))
        .withColumn("review_title", upper(trim("review_title")))
        .withColumn("source_channel", upper(trim("source_channel")))
        
        .withColumn(
            "client_id", 
            upper(trim(regexp_replace("client_id", r"[./\-\s]", "")))
        )
        
        .withColumn(
            "service_type", 
            upper(trim(regexp_replace("service_type", r"\s+", "_")))
        )
    )
    
    return df_clean

def save_data(
        df: DataFrame,
        schema_target: List[Dict[str, str]],
        table_name: str,
        target_mode: str,
        target_format: str,
        partition_value: str
):
    logger.info("Iniciando processo de gravação...")

    partition_ftm = datetime.strptime(partition_value, "%Y-%m-%d").strftime("%Y%m")
    final_df = df.withColumn("date_load", lit(partition_ftm))

    
    if final_df.rdd.isEmpty():  
        logger.warning("Nenhum dado para gravar. Verificar se houve ingestao!")
    else:
        fmt = target_format
        mode = target_mode

        writer = final_df.write.format(fmt).mode(mode).partitionBy("date_load")

        if mode == "overwrite":
            writer = writer.option("replaceWhere", f"date_load = '{partition_ftm}' ")
            logger.info(f"Sobrescrevendo partição {partition_ftm} ")

        writer.saveAsTable(table_name)

    logger.info(f"Dados gravados com sucesso na tabela {table_name}")


def optimize_delta_table(table_name: str, partition_col: str, date_load: str, zorder_cols: List[str] = None):
    logger.info(f"Iniciando otimização da tabela {table_name} para a partição {date_load}.")
    optimize_cmd = f"OPTIMIZE {table_name} WHERE {partition_col} = '{date_load}'"
    if zorder_cols and len(zorder_cols) > 0:
        zorder_clause = ", ".join(zorder_cols)
        optimize_cmd += f" ZORDER BY ({zorder_clause})"
        logger.info(f"Aplicando Z-Ordering nas colunas: {zorder_cols}")
    spark.sql(optimize_cmd)
    logger.info("Otimização concluída.")

def run_pipeline(
    spark: SparkSession,
    date_partition: str,
    application: str,
    layer_source: str,
    env: str,
    compass_config: list,
    idLoganalytics: str,
    keyLoganalytics: str
):
    try:
        # ====== CHAMADA EXTRACAO DOS VALORES DE CONFIG ======
        if compass_config and len(compass_config) > 0:
            cfg = compass_config[0]

        (
            source_format, schema_expected, create_empty_if_missing, table_name_target, table_name_target_full, 
            target_mode, target_directory, target_format, schema_target,
            schema_depara, partitionBy, rule_control, version, last_update
        ) = get_config_compass(cfg, layer_source)

        columns_apple_reviews = [c["name_column"] for c in schema_expected if c["other"] == 'apple_reviews']
        columns_internal_reviews = [c["name_column"] for c in schema_expected if c["other"] == 'internaldb_reviews']
        days_back_rule = rule_control.get("days_back", "365")
        
        df_apple = func_read_data(spark, "b_compass.apple_reviews", columns_apple_reviews, days_back_rule)
        df_internaldb = func_read_data(spark, "b_compass.internal_db", columns_internal_reviews, days_back_rule)

        # # 2. Processamento e Transformação
        df_unified = unify_data(df_apple, df_internaldb)

        df_filtered = apply_data_rules(df_unified, rule_control)
        df_transformed = processing_reviews(df_filtered)

        # 3. Validação de Qualidade de Dados
        pk_cols = [c["name_column"] for c in schema_target if c.get("is_pk") == "true"]
        validation_results = validate_data(spark=spark, df=df_transformed, compass_config=compass_config[0], primary_key=pk_cols)


        # 4. Escrita e Otimização da Tabela Delta
        table_target_name = f"b_compass.{application}"

        save_data(
            df=df_transformed,
            schema_target=schema_target,
            table_name=table_name_target_full,
            target_mode=target_mode,
            target_format=target_format,
            partition_value=date_partition
        )

        if env == "pre":
            display(df_transformed)
        
        zorder_cols = [c["name_column"] for c in schema_target if c.get("is_zorder") == "true"]
        optimize_delta_table(table_name=table_name_target_full, partition_col="date_load", date_load=date_partition, zorder_cols=zorder_cols)

        # 5. Telemetria e Logs Finais
        collector.end_collection()
        owner_data = {
            "dominio": "DOMINIO_INSTITUICAO",
            "projeto": "compass",
            "layer_lake": "silver",
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
        logger.info("Processo de ingestão e monitoramento finalizado com sucesso.")

    except Exception as e:
        logger.error(f"Falha crítica no pipeline. Erro: {e}", exc_info=True)


# COMMAND ----------

# Chamada extração dos valores de configuração
if compass_config and len(compass_config) > 0:
    cfg = compass_config[0]
else:
    dbutils.notebook.exit("Falha na leitura da tabela de controle. Nenhum registro encontrado.")

# Chama a função principal que executa todo o pipeline
run_pipeline(
    spark=spark,
    date_partition=date_partition,
    application=application,
    layer_source=layer_source,
    env=env,
    compass_config=compass_config,
    idLoganalytics=idLoganalytics,
    keyLoganalytics=keyLoganalytics
)