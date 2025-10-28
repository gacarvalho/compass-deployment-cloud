# Databricks notebook source
# ======================================================
# Importações principais
# ======================================================
import logging
from datetime import datetime
from pyspark.sql import SparkSession

# ======================================================
# Configuração de logging
# ======================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logging.getLogger("pyspark").setLevel(logging.WARNING)
logger = logging.getLogger("compass.expurge")

# ======================================================
# Widgets e parâmetros
# ======================================================
dbutils.widgets.text("date_partition", "2025-10-28")
date_partition = dbutils.widgets.get("date_partition")

if not date_partition:
    raise ValueError("O parâmetro 'date_partition' é obrigatório (ex: 2025-10-28)")

print(f"Data recebida para expurgo: {date_partition}")

# ======================================================
# Configurações do ADLS
# ======================================================
secret_scope_name = "storage_data"
storage_account_name = "compassdataprod"
container_name = "raw-compass"

# Recupera tokens e credenciais
sas_token = dbutils.secrets.get(scope=secret_scope_name, key="adlsstoragekeydata")

# Configuração Spark para acesso SAS ao ADLS Gen2
spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account_name}.dfs.core.windows.net",
               "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account_name}.dfs.core.windows.net", sas_token)

# Otimizações Delta Lake (opcional)
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

# Caminho base do container
base_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net"

# ======================================================
# Função de deleção
# ======================================================
def delete_path(path: str):
    """
    Remove recursivamente o diretório/path especificado no ADLS Gen2.
    """
    try:
        dbutils.fs.rm(path, recurse=True)
        logger.info(f"Diretório deletado com sucesso: {path}")
    except Exception as e:
        logger.warning(f"Falha ao deletar {path}: {e}")

# ======================================================
# Montagem dos caminhos a serem deletados
# ======================================================
apple_banks = ["bradesco", "itau", "nubank", "santander_br", "santander_way"]

paths_to_delete = [
    f"{base_path}/internal_db/reviews/{date_partition}"
]

# Adiciona diretórios das lojas Apple
for bank in apple_banks:
    paths_to_delete.append(f"{base_path}/apple_store/reviews/{date_partition}/{bank}")

# Diretório principal da data (após os bancos)
paths_to_delete.append(f"{base_path}/apple_store/reviews/{date_partition}")

# ======================================================
# Execução do expurgo
# ======================================================
for path in paths_to_delete:
    delete_path(path)

logger.info(" Expurgo finalizado com sucesso!")
print("Expurgo finalizado com sucesso!")
