import pymongo
import json
import logging
from pymongo import MongoClient
import os
from tempfile import NamedTemporaryFile

logger = logging.getLogger(__name__)

def _extract_and_save_documents(mongo_uri, db_name, collection_name, temp_file_path):
    """
    Função auxiliar para conectar ao MongoDB, extrair dados e salvá-los em um arquivo.
    """
    try:
        with MongoClient(mongo_uri) as client:
            db = client[db_name]
            collection = db[collection_name]
            logger.info("Conexão com o MongoDB estabelecida com sucesso.")

            with open(temp_file_path, 'w', encoding='utf-8') as temp_file:
                cursor = collection.find({})
                count = 0
                for doc in cursor:
                    doc.pop('_id', None)  # remove o _id do MongoDB
                    json_line = json.dumps(doc, ensure_ascii=False)  # mantém acentos
                    temp_file.write(json_line + "\n")
                    count += 1

            logger.info(f"Extraídos e salvos {count} documentos em '{temp_file_path}'.")
            return count

    except pymongo.errors.ConnectionFailure as e:
        logger.error(f"Erro de conexão com o MongoDB: {e}")
        raise
    except Exception as e:
        logger.error(f"Erro inesperado durante a extração: {e}")
        raise


def extract_data_from_mongo(**kwargs):
    """
    Função principal do PythonOperator que orquestra a extração e o salvamento.
    Retorna o caminho do arquivo temporário para a próxima tarefa via XComs.
    """
    mongo_uri = kwargs.get('mongo_uri')
    db_name = kwargs.get('db_name')
    collection_name = kwargs.get('collection_name')
    ti = kwargs.get('ti')

    # Cria um arquivo temporário que será limpo automaticamente após a conclusão da tarefa
    with NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
        temp_file_path = temp_file.name

    # Chama a função auxiliar para fazer a extração e salvar no arquivo
    document_count = _extract_and_save_documents(mongo_uri, db_name, collection_name, temp_file_path)

    # Exemplo de como passar o caminho do arquivo para a próxima tarefa usando XComs
    # Apenas o caminho do arquivo (uma string) é armazenado, economizando recursos.
    ti.xcom_push(key='extracted_file_path', value=temp_file_path)
    
    # Retorna o caminho para visualização no log
    return temp_file_path
