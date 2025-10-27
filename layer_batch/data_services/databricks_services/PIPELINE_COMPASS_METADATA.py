# Databricks notebook source
import pandas as pd
import requests
import yaml
import json
from io import StringIO
from pyspark.sql import SparkSession 

# --- Configurações de Segurança e URL ---
GITHUB_TOKEN = "GITHUB_TOKEN" 

RAW_YAML_URL = "https://raw.githubusercontent.com/gacarvalho/compass-deployment-cloud/main/layer_batch/data_services/others/compass_metadata.yaml"

# --- Funções de Leitura e Formatação ---
def ler_yaml_do_github(url, token):
    """Lê um arquivo YAML de uma URL raw do GitHub usando um token de acesso."""
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3.raw"
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return yaml.safe_load(StringIO(response.text))
    except Exception as e:
        print(f"Erro ao carregar ou parsear o YAML: {e}")
        return None

def formatar_config_para_lista_de_listas(config_dict):
    """Converte um dicionário de configuração de volta para o formato de lista de listas."""
    return [[key, value] for key, value in config_dict.items()]

def formatar_rule_control_para_lista_de_dicionarios(rules_list):
    """Converte a lista de regras para o formato [{"rule":"nome", "value":"valor"}, ...]"""
    return [{"rule": item.get('rule'), "value": item.get('value')} for item in rules_list]

def estruturar_em_dataframe_linha_unica(yaml_data):
    """
    Extrai todos os dados do YAML e os estrutura em um DataFrame Pandas de linha única.
    """
    if not yaml_data:
        return pd.DataFrame()

    try:
        data = yaml_data['flow_config']
        
        # 1. Extração de dados simples
        version = data['metadata']['version']
        last_modified = data['metadata']['last_modified']
        source_layer = data['data_layers']['source_layer']
        table_name_target = data['data_layers']['target_table']
        
        # 2. Extração e formatação de dados complexos (conversão para string JSON compactada)
        separators = (',', ':') # Formato JSON compactado
        
        schema_expected = json.dumps(data['schemas']['schema_expected'], separators=separators)
        schema_target = json.dumps(data['schemas']['schema_target'], separators=separators)
        schema_depara = json.dumps(data['column_mapping'], separators=separators)
        
        rule_control_list = formatar_rule_control_para_lista_de_dicionarios(data['control_rules'])
        rule_control = json.dumps(rule_control_list, separators=separators)

        source_config_list = formatar_config_para_lista_de_listas(data['configurations']['source_config'])
        source_config = json.dumps(source_config_list, separators=separators)
        
        target_config_list = formatar_config_para_lista_de_listas(data['configurations']['target_config'])
        target_config = json.dumps(target_config_list, separators=separators)
        
        fallback_config_list = formatar_config_para_lista_de_listas(data['configurations']['fallback_config'])
        fallback_config = json.dumps(fallback_config_list, separators=separators)

        # 3. Criação do dicionário final para o DataFrame
        df_dict = {
            "version": [version],
            "source_layer": [source_layer],
            "table_name_target": [table_name_target],
            "schema_expected": [schema_expected],
            "schema_target": [schema_target],
            "schema_depara": [schema_depara],
            "rule_control": [rule_control],
            "source_config": [source_config],
            "target_config": [target_config],
            "fallback_config": [fallback_config],
            "last_modified": [last_modified],
        }

        # 4. Criação do DataFrame Pandas
        df_config = pd.DataFrame(df_dict)
        
        return df_config
        
    except KeyError as e:
        print(f"Erro: Chave ausente no dicionário YAML: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Ocorreu um erro ao criar o DataFrame: {e}")
        return pd.DataFrame()


yaml_data = ler_yaml_do_github(RAW_YAML_URL, GITHUB_TOKEN)

if yaml_data is not None:
    df_config_pandas = estruturar_em_dataframe_linha_unica(yaml_data)
   
    print("--- DataFrame de Configuração Carregado com Sucesso ---")    
    display(df_config_pandas)

# COMMAND ----------

