# Databricks notebook source
# Importa bibliotecas essenciais e de tipagem
import json
import logging
import re
import warnings
import base64
import hmac
import hashlib
import requests
import pyspark.sql
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Tuple, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DataType, DecimalType
from datetime import datetime
from pyspark.sql.window import Window
from functools import reduce

# Configuração de logging e ambiente
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logging.getLogger("pyspark").setLevel(logging.WARNING)
logger = logging.getLogger("compass.ingest")

# Recupera o SAS Token do secret scope
secret_scope_name = "storage_data"
storage_account_name = "compassdataprod"

# Recupera os secrets corretamente do scope
container = "raw-compass"

# dbutils.secrets.get é assumido como existente no ambiente Databricks
idLoganalytics = dbutils.secrets.get(scope=secret_scope_name, key="customeridLoganalytics")
keyLoganalytics = dbutils.secrets.get(scope=secret_scope_name, key="keyLoganalytics")
sas_token = dbutils.secrets.get(scope=secret_scope_name, key="adlsstoragekeydata")

# Configuração do Spark para o ADLS e otimizações do Delta Lake
spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account_name}.dfs.core.windows.net",
                "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account_name}.dfs.core.windows.net", sas_token)

# Otimizações do Delta Lake
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

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

# Parâmetros de entrada do job (app_reference removido)
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


# Funções e classes de telemetria e utilitários
class ExecutionMetricsCollector:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.start_time = None
        self.end_time = None

    def start_collection(self):
        self.start_time = datetime.now()

    def end_collection(self):
        self.end_time = datetime.now()

    def collect_metrics(self, validation_results: dict, owner_data: dict, id_app: str) -> dict:
        if not self.start_time or not self.end_time:
            raise ValueError("start_collection() e end_collection() precisam ser chamados.")

        # Remove as chaves duplicadas do dicionário 'validation_results'
        valid_data_summary = validation_results.pop("valid_data")
        invalid_data_summary = validation_results.pop("invalid_data")

        total_records = validation_results["total_records"]
        total_time = (self.end_time - self.start_time).total_seconds()
        formatted_time = f"{total_time:.2f} s"

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

# Mapeamento de tipos
def resolve_spark_type(type_str: str) -> DataType:
    if not type_str: return StringType()
    t = type_str.strip().upper()
    if t.startswith("DECIMAL"):
        m = re.match(r"DECIMAL\((\d+)\s*,\s*(\d+)\)", t)
        if m: return DecimalType(int(m.group(1)), int(m.group(2)))
        return DecimalType(38, 18)
    return type_map.get(t, StringType())

def get_config_compass(cfg: pyspark.sql.Row) -> tuple:
    # converte para dict
    cfg_dict = cfg.asDict()

    schema_expected = cfg_dict["schema_expected"]
    schema_target = cfg_dict["schema_target"]
    schema_depara = cfg_dict["schema_depara"]
    table_target_vl = cfg_dict["table_name_target"]
    rule_control = cfg_dict["rule_control"]
    source_config = cfg_dict["source_config"]
    target_config = cfg_dict["target_config"]
    fallback_config = cfg_dict["fallback_config"]
    version = cfg_dict["version"]

    not_empty_rule = "false"
    evolution_mergeschema = "false"

    if rule_control and isinstance(rule_control, list):
        for rule in rule_control:
            rule_dict = rule.asDict() if hasattr(rule, "asDict") else rule
            if rule_dict.get("rule") == "not_empty":
                not_empty_rule = rule_dict.get("value", "false")
            elif rule_dict.get("rule") == "evolution_mergeschema":
                evolution_mergeschema = rule_dict.get("value", "false")

    logging.info("=== CONFIGURACOES SOURCE ===")
    logging.info("source_config: %s", json.dumps(source_config, separators=(",", ":")))
    logging.info("source_schema: %s", json.dumps([s.asDict() for s in schema_expected], separators=(",", ":")))
    
    logging.info("=== CONFIGURACOES TARGET ===")
    logging.info("target_config: %s", json.dumps(target_config, separators=(",", ":")))
    logging.info("target_schema: %s", json.dumps([s.asDict() for s in schema_target], separators=(",", ":")))
    logging.info("depara_schema: %s", json.dumps([d.asDict() for d in schema_depara], separators=(",", ":")))
    logging.info("rules: %s", json.dumps([r.asDict() for r in rule_control], separators=(",", ":")))
    
    return (
        source_config, schema_expected, target_config, schema_target, schema_depara,
        fallback_config, rule_control, version, not_empty_rule, evolution_mergeschema,
    )

def get_spark_schema_from_list(schema_list: list) -> T.StructType:
    fields = []
    for col_schema in schema_list:
        col_schema = col_schema.asDict() if hasattr(col_schema, "asDict") else col_schema
        col_name = col_schema["name_column"]
        col_type = resolve_spark_type(col_schema["type_column"])
        fields.append(T.StructField(col_name, col_type, True))
    return T.StructType(fields)

def func_read_source_from_config(
    spark: SparkSession,
    source_config: Dict[str, str],
    schema_expected: List[Dict[str, str]],
    storage_account: str,
    container: str,
    date_partition_path: str = ""
) -> DataFrame:
    
    base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{source_config['directory'].rstrip('/')}"

    paths_to_read = [f"{base_path}/{date_partition_path.strip()}"]
    
    paths_to_read_existing = []
    # Assumindo que dbutils.fs.ls está disponível (ambiente Databricks)
    for path in paths_to_read:
        try:
            dbutils.fs.ls(path)
            paths_to_read_existing.append(path)
        except Exception:
            logger.warning(f"Path não encontrado e será ignorado: {path}")

    if not paths_to_read_existing:
        logger.warning("Nenhum arquivo encontrado para leitura. Retornando DataFrame vazio.")
        return spark.createDataFrame([], schema=get_spark_schema_from_list(schema_expected))

    try:
        read_format = source_config['format'].lower() 
        
        # 1. Obter o schema Spark
        spark_schema: StructType = get_spark_schema_from_list(schema_expected)
        
        # 2. Iniciar o leitor
        reader = spark.read.format(read_format).schema(spark_schema)
        
        # 3. Add opcao de leitura recursiva
        reader = reader.option("recursiveFileLookup", "true") 
        
        # 4. Tratamento de formatos
        if read_format == "csv":
            reader = reader.option("header", "true")

        if read_format == "json":
           pass 

        df_raw = reader.load(paths_to_read_existing)
    except Exception as e:
        logger.error(f"Erro ao ler arquivos. Retornando DataFrame vazio. Erro: {e}")
        return spark.createDataFrame([], schema=get_spark_schema_from_list(schema_expected))
    
    return df_raw


def add_columns_complement(
    df: DataFrame,
    schema_target: List[Any],  # Pode vir como Row ou dict
    schema_depara: List[Any],  # Pode vir como Row ou dict
    delta_schema_map: Dict[str, Any],
    date_load: str,
    spark: SparkSession # Agora obrigatório para criar DF vazio
) -> DataFrame:
    """
    Adiciona colunas complementares ao DataFrame e garante consistência de tipos, e controle de carga.
    A coluna 'app_reference' foi removida.
    """

    CONTROL_COLUMNS = ["ingestion_ts", "date_load"] # "app_reference" removida

    # --- Função auxiliar para resolver colunas de forma segura ---
    def safe_col(df: DataFrame, col_name: str):
        """
        Retorna a coluna se existir, tenta acessar campos aninhados (ex: client.segment),
        ou cria literal nulo caso a coluna não exista.
        """
        # Caso exato (coluna simples)
        if col_name in df.columns:
            return F.col(f"`{col_name}`")  # Escapa nomes com ponto literal

        # Caso aninhado (ex: client.segment)
        if "." in col_name:
            base = col_name.split(".")[0]
            if base in df.columns:
                return F.col(col_name)

        # Fallback (coluna inexistente)
        return F.lit(None)

    # --- Verifica DataFrame vazio ---
    if df is None or len(df.head(1)) == 0:
        # Garante criação de DataFrame vazio com o schema alvo
        final_df = spark.createDataFrame([], schema=get_spark_schema_from_list(schema_target))
        # Adiciona as colunas de controle para manter a consistência do schema alvo
        final_df = (
            final_df.withColumn("ingestion_ts", F.lit(None).cast(TimestampType()))
            .withColumn("date_load", F.lit(None).cast(StringType()))
        )
        return final_df

    # --- Converte schema_depara e schema_target para dicts seguros ---
    depara_list = [r.asDict() if hasattr(r, "asDict") else r for r in schema_depara]
    map_dest_to_source = {d["target_column"]: d["source_column"] for d in depara_list}

    schema_target_list = [r.asDict() if hasattr(r, "asDict") else r for r in schema_target]

    # --- Seleciona e faz cast das colunas ---
    expressions = []
    for col_dict in schema_target_list:
        col_dest = col_dict["name_column"]

        # Ignora colunas de controle, pois serão adicionadas ao final
        if col_dest in CONTROL_COLUMNS:
            continue

        # Determina a origem
        col_src = map_dest_to_source.get(col_dest, col_dest)

        # Tipo alvo
        target_type = delta_schema_map.get(
            col_dest, resolve_spark_type(col_dict["type_column"])
        )

        # Usa safe_col para evitar erro se a coluna não existir
        expressions.append(
            safe_col(df, col_src).cast(target_type).alias(col_dest)
        )

    final_df = df.select(*expressions)

    # --- Colunas de controle ---
    final_df = (
        final_df.withColumn("ingestion_ts", F.current_timestamp().cast(TimestampType()))
        .withColumn("date_load", F.lit(date_load).cast(StringType()))
    )

    return final_df



def get_delta_schema_safe(table_name: str) -> dict:
    if spark.catalog.tableExists(table_name):
        return {f.name: f.dataType for f in spark.table(table_name).schema.fields}
    return {}
def validate_data(spark: SparkSession, df: DataFrame, compass_config=None, primary_key: list = None) -> dict:
    if primary_key is None: primary_key = []
    
    #  Acessa o objeto Row com colchetes e converte para dict ===
    rule_list = []
    if compass_config and compass_config[0] is not None:
        # Pega o primeiro item (Row) e converte para dicionário, ou usa o objeto Row se já for um dict.
        cfg_dict = compass_config[0].asDict() if hasattr(compass_config[0], "asDict") else compass_config[0]
        
        # Acessa 'rule_control' de forma segura.
        rule_control_data = cfg_dict.get("rule_control") 
        
        # Converte os Rows dentro da lista rule_control para dicts, se necessário
        if rule_control_data and isinstance(rule_control_data, list):
            rule_list = [r.asDict() if hasattr(r, "asDict") else r for r in rule_control_data]


    # ======== 1. Duplicidade ========
    if primary_key and all(c in df.columns for c in primary_key):
        window_spec = Window.partitionBy(*primary_key)
        df = df.withColumn("is_duplicate", (F.count("*").over(window_spec) > 1))
    else:
        df = df.withColumn("is_duplicate", F.lit(False))

    # ======== 2. Nulos ========
    null_cols = [r.get("column_name") for r in rule_list if r.get("rule") == "not_empty" and r.get("value", "").lower() == "true"]
    conditions = [F.col(c).isNull() | (F.trim(F.col(c)) == "") for c in null_cols if c in df.columns]
    
    if conditions:
        df = df.withColumn("is_null_issue", reduce(lambda x, y: x | y, conditions))
    else:
        df = df.withColumn("is_null_issue", F.lit(False))


    # ======== 3. Consistência de tipos ========
    df = df.withColumn("is_type_issue", F.lit(False))

    # ======== 4. Resumo em única passada ========
    if df.rdd.isEmpty():
        return {
            "total_records": 0,
            "valid_data": {"count": 0, "percentage": 0},
            "invalid_data": {"count": 0, "percentage": 0},
            "duplicate_check": {"status": True, "count": 0},
            "null_check": {"status": True, "count": 0},
            "type_consistency_check": {"status": True, "count": 0}
        }
    
    summary = df.agg(
        F.count("*").alias("total_records"),
        F.sum(F.col("is_duplicate").cast("int")).alias("duplicate_count"),
        F.sum(F.col("is_null_issue").cast("int")).alias("null_issue_count"),
        F.sum(F.col("is_type_issue").cast("int")).alias("type_issue_count")
    ).collect()[0].asDict()

    total = summary["total_records"]
    
    invalid_count = summary["duplicate_count"] + summary["null_issue_count"] + summary["type_issue_count"]
    valid = total - invalid_count
    
    return {
        "total_records": total,
        "valid_data": {"count": valid, "percentage": (valid / total) * 100 if total else 0},
        "invalid_data": {"count": invalid_count, "percentage": (invalid_count / total) * 100 if total else 0},
        "duplicate_check": {"status": summary["duplicate_count"] == 0, "count": summary["duplicate_count"]},
        "null_check": {"status": summary["null_issue_count"] == 0, "count": summary["null_issue_count"]},
        "type_consistency_check": {"status": summary["type_issue_count"] == 0, "count": summary["type_issue_count"]}
    }

def save_data(df: DataFrame, table_name: str, target_config: Dict[str, Any], date_load: str):
    logger.info("Iniciando processo de gravação...")
    if df.rdd.isEmpty():
        logger.warning("Nenhum dado para gravar. O DataFrame está vazio.")
        return
    
    # Garante que a coluna de partição existe antes de usá-la
    if "date_load" not in df.columns:
        df = df.withColumn("date_load", F.lit(date_load).cast(StringType()))
        logger.warning("Coluna 'date_load' não encontrada no DF, adicionada agora.")

    writer = df.write.format(target_config["format"]).mode(target_config["mode"]).partitionBy("date_load")
    
    if target_config["mode"] == "overwrite":
        writer = writer.option("replaceWhere", f"date_load = '{date_load}'")
        logger.info(f"Sobrescrevendo partição date_load = '{date_load}'")
        
    writer.saveAsTable(table_name)
    logger.info(f"Dados gravados com sucesso na tabela {table_name}")

def optimize_delta_table(table_name: str, partition_col: str, date_load: str, zorder_cols: List[str] = None):
    logger.info(f"Iniciando otimização da tabela {table_name} para a partição {date_load}.")
    
    # Verifica se a tabela existe antes de otimizar
    if not spark.catalog.tableExists(table_name):
         logger.warning(f"A tabela {table_name} não existe. Otimização ignorada.")
         return

    optimize_cmd = f"OPTIMIZE {table_name} WHERE {partition_col} = '{date_load}'"
    if zorder_cols and len(zorder_cols) > 0:
        zorder_clause = ", ".join(zorder_cols)
        optimize_cmd += f" ZORDER BY ({zorder_clause})"
        logger.info(f"Aplicando Z-Ordering nas colunas: {zorder_cols}")
    
    spark.sql(optimize_cmd)
    logger.info("Otimização concluída.")

# Lógica de Orquestração Principal
def run_pipeline(
    spark: SparkSession,
    date_partition: str,
    application: str,
    layer_source: str,
    env: str,
    compass_config: list,
):
    try:
        # 1. Carregamento de Configurações
        cfg = compass_config[0]
        (
            source_config, source_schema, target_config, target_schema, depara_schema,
            fallback_config, rule_control, version, not_empty_rule, evolution_mergeschema,
        ) = get_config_compass(cfg)

        # 2. Leitura e Renomeação de Dados
        df_raw = func_read_source_from_config(
            spark=spark,
            source_config=source_config,
            schema_expected=source_schema,
            storage_account=storage_account_name,
            container=container,
            date_partition_path=date_partition
        )

        # 3. Processamento e Transformação
        df_transformed = add_columns_complement(
            df=df_raw,
            schema_target=target_schema,
            schema_depara=depara_schema,
            delta_schema_map=get_delta_schema_safe(f"b_compass.{application}"),
            date_load=date_partition,
            spark=spark
        )
        
        # 4. Validação de Qualidade de Dados
        # Assumindo que o schema_target é uma lista de objetos que podem ser convertidos para dict
        pk_cols = [c.asDict()["name_column"] for c in target_schema if hasattr(c, "asDict") and c.asDict().get("is_pk")]
        validation_results = validate_data(
            spark=spark,
            df=df_transformed,
            compass_config=compass_config,
            primary_key=pk_cols
        )

        # 5. Escrita e Otimização da Tabela Delta
        table_target_name = f"b_compass.{application}"
        save_data(
            df=df_transformed,
            table_name=table_target_name,
            target_config=target_config,
            date_load=date_partition
        )

        zorder_cols = [c.asDict()["name_column"] for c in target_schema if hasattr(c, "asDict") and c.asDict().get("is_zorder")]
        optimize_delta_table(
            table_name=table_target_name,
            partition_col="date_load",
            date_load=date_partition,
            zorder_cols=zorder_cols
        )

        # 6. Telemetria e Logs Finais
        collector.end_collection()
        owner_data = {
            "dominio": "DOMINIO_INSTITUICAO",
            "projeto": "compass",
            "layer_lake": "bronze",
            "layer_source": layer_source
        }

        final_metrics = collector.collect_metrics(
            validation_results=validation_results,
            owner_data=owner_data,
            id_app=application # Usando 'application' no lugar de 'app_reference'
        )

        if env == "pre":
            final_json_log = json.dumps(final_metrics, indent=2)
            logger.info("Métricas finais de execução:\n" + final_json_log)

        send_to_log_analytics(log_data=final_metrics, log_type="CompassLogs_CL")
        logger.info("Processo de ingestão e monitoramento finalizado com sucesso.")

    except Exception as e:
        logger.error(f"Falha crítica no pipeline. Erro: {e}", exc_info=True)

# Inicia a execução do pipeline
if compass_config:
    collector = ExecutionMetricsCollector(spark)
    collector.start_collection()
    run_pipeline(
        spark=spark,
        date_partition=date_partition,
        application=application,
        layer_source=layer_source,
        env=env,
        compass_config=compass_config,
    )