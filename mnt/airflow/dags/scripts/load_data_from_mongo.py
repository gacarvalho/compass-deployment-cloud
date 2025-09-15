import random
import uuid
from datetime import datetime, timezone
from faker import Faker
from pymongo import MongoClient
from itertools import product

fake = Faker("pt_BR")

# Configurações
CLIENT_CLASSIFICATIONS = ["Baixo", "Médio", "Alto", "Enterprise", "Estudante", "Sênior"]
CLIENT_SEGMENTS = ["pf", "pj"]
SERVICE_TYPES = ["cartão de crédito", "empréstimo", "conta corrente", "seguro"]
SOURCE_CHANNELS = ["website", "mobile_app", "agência", "call_center"]

# Nomes de apps disponíveis
SERVICE_APP = ["INSTITUICAO_APP_BR", "INSTITUICAO_APP_GLOBAL", "INSTITUICAO_APP_SEGUROS", "INSTITUICAO_APP_FINANCIAMENTO","INSTITUICAO_APP_EMPRESAS","INSTITUICAO_APP_CONTA_MEI","INSTITUICAO_APP_CONTA_CORRENTE"]

# Templates para comentários
TEMPLATES = [
    "O {service} foi {adjective} e {verb}.",
    "Experiência com {service} {verb} {adverb}.",
    "O atendimento do {service} foi {adjective}, {adverb}.",
    "Tive problemas com {service}, {verb} {adverb}.",
    "Recomendo o {service} porque {verb} {adverb}.",
]

ADJECTIVES = ["excelente", "ruim", "rápido", "lento", "eficiente", "confuso"]
VERBS = ["funcionou", "não funcionou", "demorou", "resolveu meu problema", "gerou complicações"]
ADVERBS = ["rapidamente", "lentamente", "com facilidade", "sem problemas", "muito mal"]

def generate_unique_comments(num_comments):
    """Gera comentários únicos combinando templates, adjetivos, verbos, advérbios e apps."""
    all_combinations = list(product(TEMPLATES, SERVICE_TYPES, ADJECTIVES, VERBS, ADVERBS, SERVICE_APP))
    random.shuffle(all_combinations)
    
    comments = []
    for tpl, service, adj, verb, adv, app in all_combinations[:num_comments]:
        comment = tpl.format(service=service, adjective=adj, verb=verb, adverb=adv)
        comments.append((comment, app))  # Retorna também o app associado
    
    return comments

def insert_fake_feedbacks(mongo_uri, db_name, collection_name, num_feedbacks=689):
    if not mongo_uri:
        raise ValueError("Variável de ambiente MONGO_URI não configurada!")

    comments_with_apps = generate_unique_comments(num_feedbacks)

    feedbacks = []
    for comment, app in comments_with_apps:
        client_segment = random.choice(CLIENT_SEGMENTS)
        classification = random.choice(CLIENT_CLASSIFICATIONS)
        services = random.choice(SERVICE_TYPES)

        doc = {
            "_id": uuid.uuid4().hex[:24],
            "submission_date": datetime.now(timezone.utc).isoformat(),
            "client": {
                "segment": client_segment,
                "identification": fake.cnpj() if client_segment == "pj" else fake.cpf(),
                "classification": classification
            },
            "feedback_rating": random.randint(1, 5),
            "feedback_comment": comment,
            "service_type": services,
            "service_id": f"PROD-{random.randint(100, 999)}",
            "service_feedback": services,
            "source_channel": random.choice(SOURCE_CHANNELS),
            "source_id": fake.word(),
            "app_reference": app,
            "source_user_agent": fake.user_agent()
        }
        feedbacks.append(doc)

    with MongoClient(mongo_uri) as client:
        db = client[db_name]
        collection = db[collection_name]
        collection.insert_many(feedbacks)

    return f"{num_feedbacks} avaliações únicas em português inseridas na coleção '{collection_name}' do DB '{db_name}'"
