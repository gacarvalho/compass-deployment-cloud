# Databricks notebook source
# Realiza import necessarias
import json
import logging
import re
import warnings
import yaml
import pyspark.sql
from datetime import datetime, date, timedelta
from string import Template
from typing import Any, Dict, List, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
    DataType,
    DecimalType
)
from pyspark.sql.functions import col, count, when, lit, current_timestamp

# COMMAND ----------

# Realiza configuracao de logging, storage e token

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logging.getLogger("pyspark").setLevel(logging.WARNING)
logger = logging.getLogger("compass.apple")

storage_account_name = "compassdataprod"
container = "sa-compasslake"
secret_scope_name = "adlsscpkeydata"
secret_key_name   = "adlsstoragekeydata"

# Recupera o SAS Token do secret scope
sas_token = dbutils.secrets.get(scope=secret_scope_name, key=secret_key_name)

# Configuração com SAS Token
spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account_name}.dfs.core.windows.net", sas_token)
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")



# COMMAND ----------

class ExecutionMetricsCollector:
    """
    Coleta métricas de execução Spark (sem dependência de sparkmeasure).
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.start_time = None
        self.end_time = None

    def start_collection(self):
        """Marca início do processo"""
        self.start_time = datetime.now()

    def end_collection(self):
        """Marca fim do processo"""
        self.end_time = datetime.now()

    def collect_metrics(
        self,
        valid_df: DataFrame,
        invalid_df: DataFrame,
        validation_results: dict,
        id_app: str,
        layer_lake: str
    ) -> str:
        """
        Gera JSON com métricas da execução.
        """
        if not self.start_time or not self.end_time:
            raise ValueError("start_collection() e end_collection() precisam ser chamados.")

        # Tempo de execução
        total_time = (self.end_time - self.start_time).total_seconds()
        formatted_time = f"{total_time:.2f} s"

        # Contagens
        count_valid = valid_df.count()
        count_invalid = invalid_df.count()
        total_records = count_valid + count_invalid
        percentage_valid = (count_valid / total_records * 100) if total_records > 0 else 0.0

        # Monta dicionário de métricas (GENÉRICO)
        metrics = {
            "owner": {
                "sigla": "DT",
                "projeto": "compass",
                "layer_lake": f"{layer_lake}"
            },
            "valid_data": {"count": count_valid, "percentage": percentage_valid},
            "invalid_data": {
                "count": count_invalid,
                "percentage": (count_invalid / total_records * 100) if total_records > 0 else 0.0,
            },
            "total_records": total_records,
            "total_processing_time": formatted_time,
            "validation_results": validation_results,
            "success_count": sum(1 for v in validation_results.values() if isinstance(v, dict) and v.get("status")),
            "error_count": sum(1 for v in validation_results.values() if isinstance(v, dict) and not v.get("status")),
            "_ts": {
                "compass_start_ts": self.start_time.strftime("%Y-%m-%d %H:%M:%S"),
                "compass_end_ts": self.end_time.strftime("%Y-%m-%d %H:%M:%S"),
            },
            "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "app_id": id_app,
        }

        return json.dumps(metrics, indent=2)


def validate_data(
    spark: SparkSession,
    df: DataFrame,
    compass_config=None,
    ignore_columns: list = None,
    primary_key=None  # Opcional, pode ser str ou lista de colunas
) -> tuple:
    """
    Valida um DataFrame de ingestão com base em regras de negócio e contrato (compass_config).

    Args:
        spark (SparkSession): sessão Spark.
        df (DataFrame): DataFrame a ser validado.
        compass_config: lista de Rows (contrato da tabela, contém rule_control).
        ignore_columns (list): colunas a ignorar nas validações.
        primary_key (str ou list, opcional): coluna(s) para validar duplicidade.

    Returns:
        tuple: (valid_records, invalid_records, validation_results)
    """
    if ignore_columns is None:
        ignore_columns = []

    # Normaliza primary_key para lista
    if isinstance(primary_key, str):
        primary_key = [primary_key]
    elif primary_key is None:
        primary_key = []

    validation_results = {
        "duplicate_check": {"message": None, "status": None, "code": None},
        "null_check": {"message": None, "status": None, "code": None},
        "type_consistency_check": {"message": None, "status": None, "code": None},
        "total_records": df.count(),
    }

    # 1. Verificação de duplicidade
    if primary_key:
        # garante que todas as colunas existam no DF
        pk_cols = [col for col in primary_key if col in df.columns]
        if not pk_cols:
            pk_cols = df.columns
            duplicate_msg = "Duplicatas encontradas considerando todas as colunas (nenhuma PK valida encontrada)."
        else:
            duplicate_msg = f"Duplicatas encontradas com base na chave primária {pk_cols}."
    else:
        pk_cols = df.columns
        duplicate_msg = "Duplicatas encontradas considerando todas as colunas."

    duplicates = df.groupBy(pk_cols).count().filter(F.col("count") > 1)
    duplicate_count = duplicates.count()

    if duplicate_count > 0:
        validation_results["duplicate_check"].update({
            "status": False,
            "code": 409,
            "message": f"{duplicate_msg} Total: {duplicate_count} registros."
        })
    else:
        validation_results["duplicate_check"].update({
            "status": True,
            "code": 200,
            "message": "Nenhum registro duplicado encontrado."
        })

    # 2. Verificação de nulos (via rules do compass_config)
    null_issues = {}
    rule_list = []

    if compass_config:
        raw_rules = compass_config[0]["rule_control"]
        # Se vier como string JSON
        if isinstance(raw_rules, str):
            try:
                rule_list = json.loads(raw_rules)
            except Exception as e:
                logging.warning(f"Nao foi possível interpretar rule_control: {raw_rules} ({e})")
                rule_list = []
        # Se vier como Row, converte para dict
        elif isinstance(raw_rules, pyspark.sql.types.Row):
            rule_list = [raw_rules.asDict()]
        # Se já for lista de Rows
        elif isinstance(raw_rules, list) and all(isinstance(r, pyspark.sql.types.Row) for r in raw_rules):
            rule_list = [r.asDict() for r in raw_rules]
        # Se já for lista de dicts
        else:
            rule_list = raw_rules

    for rule_dict in rule_list:
        rule = rule_dict.get("rule")
        value = rule_dict.get("value")

        if rule == "not_empty" and value.lower() == "true":
            for col in df.columns:
                if col in ignore_columns:
                    continue
                null_count = df.filter(F.col(col).isNull() | (F.col(col) == "")).count()
                if null_count > 0:
                    null_issues[col] = null_count
                    logging.warning(
                        f"Regra not_empty violada na coluna '{col}': {null_count} nulos/vazios encontrados"
                    )

    if null_issues:
        validation_results["null_check"].update({
            "status": False,
            "code": 400,
            "message": f"Valores nulos encontrados: {null_issues}"
        })
    else:
        validation_results["null_check"].update({
            "status": True,
            "code": 200,
            "message": "Nenhum valor nulo encontrado (considerando regras aplicadas)."
        })

    # 3. Consistência de tipos (placeholder)
    validation_results["type_consistency_check"].update({
        "status": False,
        "code": 0,
        "message": "Nenhuma validacao de tipo definida"
    })

    # 4. Separação registros válidos e inválidos
    invalid_records = spark.createDataFrame([], df.schema)

    if null_issues:
        for col_name in null_issues.keys():
            invalid_records = invalid_records.union(
                df.filter(F.col(col_name).isNull() | (F.col(col_name) == ""))
            )

    if duplicate_count > 0:
        duplicate_records = df.join(duplicates.select(*pk_cols), on=pk_cols, how="inner")
        invalid_records = invalid_records.union(duplicate_records)

    valid_records = df.subtract(invalid_records)

    return valid_records, invalid_records, validation_results


# COMMAND ----------

# ===================== Variaveis de entrada via param
date_partition = dbutils.widgets.get("date_partition")
app_reference  = dbutils.widgets.get("app_reference")
application    = dbutils.widgets.get("application")
layer_source   = dbutils.widgets.get("layer_source")


params = {
    "date_partition": date_partition,
    "app_reference": app_reference,
    "application": application,
    "layer_source": layer_source
}

logging.info("Parâmetros de entrada: %s", json.dumps(params))

# ===================== codigo
data_control = "control_params_compass.data_config"
partition_col = "date_load"

# Instanciar coletor - coleta métricas
collector = ExecutionMetricsCollector(spark)
collector.start_collection()

# ===================== Lê a configuração mais recente da tabela de controle
compass_config = spark.read.table(data_control) \
                           .filter((F.col("source_layer") == layer_source) &
                                   (F.col("table_name_target") == application)) \
                           .orderBy(F.desc("version")) \
                           .limit(1) \
                           .collect()
if not compass_config:
    raise ValueError(f"Nenhuma configuração encontrada para layer {layer_source} e aplicação {application}")



# COMMAND ----------

def get_config_compass(cfg):
    """
    Exibe as configurações via logging e retorna cada variável separadamente.
    Suporta PySpark Row ou dict.

    Args:
        cfg (Row ou dict): configuração do compass_config[0]

    Returns:
        tuple: todas as variáveis em ordem
    """
    # Se for Row do PySpark, converte para dict
    if hasattr(cfg, "asDict"):
        cfg = cfg.asDict()

    schema_expected = cfg["schema_expected"]   # lista de colunas esperadas
    schema_target   = cfg["schema_target"]     # lista de colunas finais
    schema_depara   = cfg["schema_depara"]     # lista de-para colunas origem => destino
    table_target_vl = cfg["table_name_target"] # nome da tabela de destino
    rule_control    = cfg["rule_control"]      # regras de validação
    source_config   = cfg["source_config"]     # config da camada raw
    target_config   = cfg["target_config"]     # config do destino
    fallback_config = cfg["fallback_config"]   # fallback
    version         = cfg["version"]           # version

    source_config_dict      = compass_config[0]["source_config"]
    table_name_target       = compass_config[0]["table_name_target"]
    schema_source_dict      = compass_config[0]["schema_expected"]    
    schema_target_dict      = compass_config[0]["schema_target"]
    schema_depara_dict      = compass_config[0]["schema_depara"]


    # Inicializa com default como "false"
    not_empty_rule = "false"
    evolution_mergeschema = "false"

    # Itera sobre a lista de regras (Row ou dict)
    if rule_control and isinstance(rule_control, list):
        for rule in rule_control:
            # Converte Row para dict se necessário
            if hasattr(rule, "asDict"):
                rule_dict = rule.asDict()
            else:
                rule_dict = rule
            
            # Atualiza valores se a regra existir
            if rule_dict.get("rule") == "not_empty":
                not_empty_rule = rule_dict.get("value", "false")
            elif rule_dict.get("rule") == "evolution_mergeschema":
                evolution_mergeschema = rule_dict.get("value", "false")




    # Atribuicao a variaveis de acordo com a configuração: SOURCE
    directory_application   = source_config_dict["directory"]
    format                  = source_config_dict["format"]
    create_empty_if_missing = fallback_config["create_empty_if_missing"]
    source_schema           = schema_source_dict

    # Atribuicao a variaveis de acordo com a configuração: TARGET
    target_schema           = schema_target_dict
    depara_schema           = schema_depara_dict
    target_table            = table_target_vl
    target_mode             = target_config["mode"]
    target_directory        = target_config["directory"]
    target_format           = target_config["format"]
    partitionBy             = target_config["partitionBy"]
    version                 = cfg["version"]
    last_update             = cfg["last_modified"]
    evolution_mergeschema   = evolution_mergeschema

    # Logging estruturado
    logging.info("=== CONFIGURACOES SOURCE ===")
    logging.info("directory_application: %s", directory_application)
    logging.info("source_format: %s", format)
    logging.info("create_empty_if_missing: %s", create_empty_if_missing)
    logging.info("source_schema: %s", json.dumps(schema_source_dict, separators=(",", ":")))

    logging.info("=== CONFIGURACOES TARGET ===")
    logging.info("target_directory: %s", target_directory)
    logging.info("target_table: %s", target_table)
    logging.info("target_mode: %s", target_mode)
    logging.info("target_format: %s", target_format)
    logging.info("partitionBy: %s", partitionBy)
    logging.info("target_schema: %s", json.dumps(target_schema, separators=(",", ":")))
    logging.info("not_empty_rule: %s", not_empty_rule)
    logging.info("evolution_mergeschema: %s", evolution_mergeschema)


    logging.info("=== OUTRAS INFORMACOES ===")
    logging.info("version: %s", version)
    logging.info("last_update_control: %s", last_update)
    logging.info("depara_schema: %s", json.dumps(schema_depara, separators=(",", ":")))

    # Retorna todas as variáveis separadamente
    return (
        directory_application,
        format,
        source_schema,
        create_empty_if_missing,
        target_table,
        target_mode,
        target_directory,
        target_format,
        target_schema,
        depara_schema,
        partitionBy,
        not_empty_rule,
        evolution_mergeschema,
        version,
        last_update
    )

# ====== CHAMADA EXTRACAO DOS VALORES DE CONFIG ======
if compass_config and len(compass_config) > 0:
    cfg = compass_config[0]

    (
        directory_application,
        source_format,
        source_schema,
        create_empty_if_missing,
        target_table,
        target_mode,
        target_directory,
        target_format,
        target_schema,
        depara_schema,
        partitionBy,
        not_empty_rule,
        evolution_mergeschema,
        version,
        last_update_control
    ) = get_config_compass(cfg)
else:
    raise ValueError("compass_config está vazio. Verifique se os dados de configuração foram carregados.")

directory_application, source_format, source_schema, create_empty_if_missing, target_table, target_mode, target_directory, target_format, target_schema, depara_schema, partitionBy, not_empty_rule, evolution_mergeschema, version, \
last_update_control = get_config_compass(cfg)


# COMMAND ----------


# Mapas de tipos Spark
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

def get_spark_schema_from_list(schema_list: list, type_map: dict) -> T.StructType:
    fields = []
    for col_schema in schema_list:
        if hasattr(col_schema, "asDict"):
            col_schema = col_schema.asDict()
        col_name = col_schema["name_column"]
        col_type_str = col_schema["type_column"].upper()
        col_type = type_map.get(col_type_str, T.StringType())
        fields.append(T.StructField(col_name, col_type, True))
    return T.StructType(fields)


def func_read_source_from_config(
        spark: SparkSession,
        source_config: dict,
        app_reference: str,
        schema_expected: List[Dict[str, str]],
        schema_depara: List[Dict[str, str]],
        storage_account: str,
        container: str,
        date_partition_path: str = "",
        days_back: int = 0,
        type_map: dict = type_map
) -> DataFrame:
    """
    Lê dados da origem aplicando schema esperado e regras de de-para
    apenas para renomear colunas. Cast de tipos será feito posteriormente.
    """

    base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{source_config['directory']}"
    fmt = source_config["format"].lower()

    # Calcula paths a ler
    if days_back > 0:
        today = datetime.today()
        date_paths = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days_back)]
        paths = [f"{base_path}{date}/{app_reference}" for date in date_paths]
    else:
        paths = [f"{base_path}{date_partition_path}/{app_reference}"]

    # Filtra paths existentes
    existing_paths = []
    for p in paths:
        try:
            _ = dbutils.fs.ls(p)
            existing_paths.append(p)
        except Exception:
            logger.warning(f"Caminho não existe ou inacessível: {p}")

    if not existing_paths:
        logger.warning("Nenhum caminho existente para leitura. Retornando DataFrame vazio.")
        return spark.createDataFrame([], schema=get_spark_schema_from_list(schema_expected, type_map))

    # Leitura inicial
    try:
        if fmt == "csv":
            header = source_config.get("header", "true").lower() == "true"
            df = spark.read.format(fmt) \
                .option("delimiter", source_config.get("delimiter", ",")) \
                .option("header", header) \
                .option("encoding", source_config.get("encoding", "utf-8")) \
                .option("mode", source_config.get("mode", "PERMISSIVE")) \
                .schema(get_spark_schema_from_list(schema_expected, type_map)) \
                .load(existing_paths)

        elif fmt == "json":
            df_temp = spark.read.json(existing_paths)
            if "entry" in df_temp.columns:
                df_temp = df_temp.select(F.explode("entry").alias("entry")).select("entry.*")

            # Mapeia colunas de acordo com schema_expected
            select_exprs = []
            for col_schema in schema_expected:
                col_schema_dict = col_schema.asDict() if hasattr(col_schema, "asDict") else col_schema
                col_name = col_schema_dict["name_column"]
                json_path_raw = col_schema_dict.get("other")

                if json_path_raw:
                    parts = json_path_raw.split(".")
                    expr = F.col(parts[0])
                    for p in parts[1:]:
                        expr = expr.getItem(p)
                    select_exprs.append(expr.alias(col_name))
                else:
                    select_exprs.append(F.col(col_name))

            try:
                df = df_temp.select(*select_exprs)
            except AnalysisException:
                # fallback: insere null se faltar coluna
                select_exprs_safe = []
                for col_schema in schema_expected:
                    col_schema_dict = col_schema.asDict() if hasattr(col_schema, "asDict") else col_schema
                    col_name = col_schema_dict["name_column"]
                    json_path_raw = col_schema_dict.get("other")
                    if json_path_raw:
                        select_exprs_safe.append(F.lit(None).alias(col_name))
                    else:
                        select_exprs_safe.append(F.col(col_name))
                df = df_temp.select(*select_exprs_safe)

        else:
            df = spark.read.format(fmt) \
                .schema(get_spark_schema_from_list(schema_expected, type_map)) \
                .load(existing_paths)

    except AnalysisException as e:
        logger.warning(f"Erro ao ler arquivos existentes {existing_paths}: {e}")
        return spark.createDataFrame([], schema=get_spark_schema_from_list(schema_expected, type_map))

    # === Aplicação de DE-PARA apenas para renomear colunas ===
    for mapping in schema_depara:
        source_col = mapping["source_column"]
        target_col = mapping["target_column"]

        if source_col not in df.columns:
            logger.warning(f"Coluna de origem '{source_col}' não encontrada. Ignorando renomeação para '{target_col}'")
            continue

        if source_col != target_col:
            df = df.withColumnRenamed(source_col, target_col)
            logger.info(f"Coluna renomeada '{source_col}' => '{target_col}'")

    # Remove registros corrompidos
    if "_corrupt_record" in df.columns:
        df_clean = df.filter(F.col("_corrupt_record").isNull())
        if df_clean.count() < df.count():
            logger.warning(f"{df.count() - df_clean.count()} registros corrompidos foram removidos.")
    else:
        df_clean = df

    return df_clean



df = func_read_source_from_config(
    spark=spark,
    source_config=compass_config[0]["source_config"],
    app_reference=app_reference,
    schema_expected=source_schema,
    schema_depara=depara_schema,
    storage_account=storage_account_name,
    container=container,
    days_back=7
)

df.printSchema()

# COMMAND ----------

# ================= Funções auxiliares =================

def resolve_spark_type(type_str: str) -> DataType:
    if not type_str:
        return StringType()
    t = type_str.strip().upper()
    if t.startswith("DECIMAL"):
        m = re.match(r"DECIMAL\((\d+)\s*,\s*(\d+)\)", t)
        if m:
            return DecimalType(int(m.group(1)), int(m.group(2)))
        return DecimalType(38, 18)
    return type_map.get(t, StringType())

def get_delta_schema_safe(table_name: str) -> dict:
    if spark.catalog.tableExists(table_name):
        return {f.name: f.dataType for f in spark.table(table_name).schema.fields}
    return {}

# ================= Função central =================

def add_columns_complement(
        df: DataFrame,
        schema_target: List[Dict[str, str]],
        schema_depara: List[Any],
        delta_schema_map: dict,
        app_reference: str,
        date_load: str
) -> DataFrame:
    """
    Garante que o DataFrame final tenha todas as colunas do schema_target,
    aplicando de-para (destino -> origem) e cast de tipos.
    Colunas de controle (app_reference, ingestion_ts, date_load) são adicionadas ao final.

    Regras:
      - Se houver de-para para a coluna destino, usa a coluna de ORIGEM indicada.
      - Se NÃO houver de-para:
           * se destino existe no DF -> segue normal;
           * caso contrário -> ERRO.
    """
    CONTROL_COLS = {"app_reference", "ingestion_ts", "date_load"}
    expressions = []

    # Normaliza schema_depara em lista de tuplas (destino, origem)
    depara_list = []
    for r in (schema_depara or []):
        if isinstance(r, (list, tuple)) and len(r) == 2:
            source, target = r
            depara_list.append((target, source))  # destino -> origem
        elif isinstance(r, dict):
            depara_list.append((r["target_column"], r["source_column"]))
        elif hasattr(r, "asDict"):
            r_dict = r.asDict()
            depara_list.append((r_dict["target_column"], r_dict["source_column"]))
        else:
            raise ValueError(f"Formato inválido em schema_depara: {r}")

    # Cria mapa DESTINO -> ORIGEM
    map_dest_to_source = {dest: src for dest, src in depara_list}

    for item in schema_target:
        item_dict = item.asDict() if hasattr(item, "asDict") else item
        col_dest = item_dict["name_column"]

        if col_dest in CONTROL_COLS:
            continue

        # Descobre a coluna de origem a usar
        col_src = map_dest_to_source.get(col_dest, col_dest)

        # Se a coluna de origem não existe, mas a coluna destino já existe no DF, usa ela
        if col_src not in df.columns:
            if col_dest in df.columns:
                col_src = col_dest
            else:
                raise ValueError(f"Coluna de origem '{col_src}' para destino '{col_dest}' não encontrada no DataFrame.")

        # Resolve tipo alvo
        target_type = delta_schema_map.get(col_dest, resolve_spark_type(item_dict["type_column"]))
        current_type = df.schema[col_src].dataType

        expressions.append(F.col(col_src).cast(target_type).alias(col_dest))

        # Logging
        if col_src == col_dest:
            logging.info(f"Coluna '{col_dest}': {current_type} => {target_type}")
        else:
            logging.info(f"Coluna '{col_dest}' (origem: '{col_src}'): {current_type} => {target_type}")

    # Colunas de controle
    expressions.append(F.lit(app_reference.lower()).alias("app_reference"))
    expressions.append(F.current_timestamp().alias("ingestion_ts"))
    expressions.append(F.lit(date_load).alias("date_load"))

    return df.select(*expressions)


def save_data(
        df: DataFrame,
        schema_target: List[Dict[str, str]],
        schema_depara: List[Any],
        table_name: str,
        target_config: Dict[str, Any],
        app_reference: str,
        date_load: str
):
    logger.info("Iniciando processo de gravação...")

    delta_schema_map = get_delta_schema_safe(table_name)

    df_final = add_columns_complement(
        df=df,
        schema_target=schema_target,
        schema_depara=schema_depara,
        delta_schema_map=delta_schema_map,
        app_reference=app_reference,
        date_load=date_load
    )

    num_rows = df_final.count()
    avg_row_size_bytes = 1024
    estimated_size_bytes = num_rows * avg_row_size_bytes

    if estimated_size_bytes < 64 * 1024 * 1024:
        df_final = df_final.coalesce(1)
        logger.info(f"DataFrame pequeno ({estimated_size_bytes / 1024**2:.2f} MB), aplicando coalesce(1)")
    else:
        logger.info(f"DataFrame grande ({estimated_size_bytes / 1024**2:.2f} MB), mantendo repartições normais")

    if num_rows == 0:
        logger.warning("Nenhum dado para gravar.")
        return

    if df_final.rdd.isEmpty():  
        logger.warning("Nenhum dado para gravar. Verificar se houve ingestao!")
    else:
        fmt = target_config.get("format", "delta")
        mode = target_config.get("mode", "append").lower()

        writer = df_final.write.format(fmt).mode(mode).partitionBy("date_load")

        if mode == "overwrite":
            writer = writer.option("replaceWhere", f"date_load = '{date_load}'")
            logger.info(f"Sobrescrevendo partição date_load = '{date_load}'")

        writer.saveAsTable(table_name)
    logger.info(f"Dados gravados com sucesso na tabela {table_name}")


# ===== CHAMADA DE EXEMPLO =====
table_target_name = f"b_compass.{target_table}"

save_data(
    df=df,
    schema_target=compass_config[0]["schema_target"],
    schema_depara=compass_config[0]["schema_depara"],
    table_name=table_target_name,
    target_config=compass_config[0]["target_config"],
    app_reference=app_reference,
    date_load=date_partition
)


logger.info("Processo de ingestão finalizado, iniciando o trabalho de coleta de métricas!")

# Exemplo de validação
valid_df, invalid_df, results = validate_data(
    spark=spark,
    df=df,
    compass_config=compass_config
)

collector.end_collection()


# COMMAND ----------

# Mapeamento target_table -> camada da arquitetura
LAYER_MAP = {
    "raw": "LANDING",
    "b_compass": "BRONZE",
    "s_compass": "SILVER",
    "g_compass": "GOLD",
    "h_compass": "HARMONIZATION",
}

def map_layer(target_table: str) -> str:
    return LAYER_MAP.get(target_table, f"UNKNOWN_{target_table}".upper())


metrics_json = collector.collect_metrics(
                            valid_df=valid_df,
                            invalid_df=invalid_df,
                            validation_results=results,
                            id_app="{}".format(app_reference),
                            layer_lake="{}".format(map_layer(layer_source))
                        )

print(metrics_json)