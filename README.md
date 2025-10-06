# 🧭 ♨️ COMPASS: Solução de Analytics para Experiência do Cliente

---

<p align="left">
  <img src="https://img.shields.io/badge/projeto-Compass-blue?style=flat-square" alt="Projeto">
  <img src="https://img.shields.io/badge/status-deployed-green?style=flat-square" alt="Status">
  <img src="https://img.shields.io/badge/autor-Gabriel_Carvalho-lightgrey?style=flat-square" alt="Autor">
</p>

O repositório **compass-deployment-cloud** é uma solução desenvolvida no contexto do programa **Data Master**, promovido pela F1rst Tecnologia. Seu objetivo é disponibilizar uma plataforma robusta e escalável para **captura, processamento e análise de feedbacks de clientes**.

![<data-master-compass>](https://github.com/gacarvalho/repo-spark-delta-iceberg/blob/main/header.png?raw=true)

Este documento apresenta a visão geral do projeto, abrangendo desde os objetivos iniciais até a descrição técnica da arquitetura, fluxos funcionais, tecnologias empregadas, instruções para execução e considerações finais. A proposta é oferecer um panorama completo sobre o funcionamento do Compass como **produto de analytics voltado à experiência do cliente**.

---

1.  [**Objetivo do Projeto**](#1-objetivo-do-projeto)
    * [1.1 O Projeto Compass](#11-o-projeto-compass)
2.  [**Arquitetura da Solução (Visão Geral)**](#2-arquitetura-da-solução-visão-geral)
3.  [**Visão Geral da Arquitetura Técnica**](#3-visão-geral-da-arquitetura-técnica)
    * [3.1 Descrição do Fluxo de Dados](#31-descrição-do-fluxo-de-dados)
        * [3.1.1 Fonte (Datasource) de Dados](#311-fonte-datasource-de-dados)
        * [3.1.2 Camada de Processamento](#312-camada-de-processamento)
        * [3.1.3 Camada de Armazenamento](#313-camada-de-armazenamento)
        * [3.1.4 Camada de Visualização e Telemetria (Observabilidade)](#314-camada-de-visualização-e-telemetria-observabilidade)
    * [3.2 Aspectos Técnicos do Projeto Compass](#32-aspectos-técnicos-do-projeto-compass)
        * [3.2.1 Tecnologias Utilizadas](#321-tecnologias-utilizadas)
        * [3.2.2 Características da Execução do Projeto](#322-características-da-execução-do-projeto)
        * [3.2.2.1 Infraestrutura do Projeto Compass (Azure)](#3221-infraestrutura-do-projeto-compass-azure)
        * [3.2.2.2 Aplicações do Projeto Compass (Batch)](#3222-aplicações-do-projeto-compass-batch)
        * [3.2.2.3 Pipeline do Projeto Compass (Airflow)](#3223-pipeline-do-projeto-compass-airflow)
4.  [**Fluxo Funcional e Jornada do Cliente**](#4-fluxo-funcional-e-jornada-do-cliente)
5.  [**Compass como Produto Analytics para a Instituição**](#5-compass-como-produto-analytics-para-a-instituição)
    * [5.1 Regras de Negócio](#51-regras-de-negócio)
    * [5.2 Dicionário de Dados](#52-dicionário-de-dados)
        * [`b_compass.apple_reviews` (Bronze)](#b_compassapple_reviews-bronze)
        * [`b_compass.internal_db` (Bronze)](#b_compassinternal_db-bronze)
        * [`s_compass.instituicao_reviews` (Silver)](#s_compassinstituicao_reviews-silver)
        * [`g_compass.reviews_customer_compass` (Gold)](#g_compassreviews_customer_compass-gold)
        * [`metadata_compass.data_params` (Controle)](#metadata_compassdata_params-controle)
6.  [**Melhorias e Considerações Finais**](#7-melhorias-e-considerações-finais)

---

# 1. Objetivo do Projeto

A idealização deste case surgiu da necessidade de fortalecer o alinhamento entre o time de negócios e a Engenharia de Dados, com foco na resolução de desafios práticos relacionados à **jornada do usuário**. A iniciativa teve como ponto de partida a ausência de visibilidade aprofundada sobre a forma como os clientes interagem com os produtos e serviços da empresa.

Diante desse cenário, o objetivo central foi desenvolver uma solução capaz de **capturar, tratar e estruturar dados de interação dos usuários**, viabilizando análises confiáveis e acionáveis para suporte à tomada de decisão. A arquitetura foi desenhada com foco em flexibilidade e escalabilidade, permitindo sua aplicação em diferentes contextos e ampliando o potencial de geração de valor, inclusive para comparação com padrões comportamentais de outras empresas do setor.

## 1.1 O Projeto Compass

O Projeto **Data Master Compass** é uma iniciativa de Engenharia de Dados projetada para capturar e analisar **feedbacks de clientes** sobre produtos e serviços. O nome **Compass** reflete seu propósito: **orientar** o time de negócios na melhoria contínua de processos e soluções, com base em dados reais.

A solução centraliza as informações em um **Data Lake no ambiente Cloud Azure**, organizando os dados por data de referência e segmento de público. Isso proporciona *insights* valiosos para **Product Owners, Product Managers e Gerentes de Projetos**, permitindo decisões baseadas em evidências e alinhadas às necessidades reais dos usuários.

> [!NOTE]
> 🧭 **Por que o nome "Compass"?**
> O nome Compass (em português, bússola) foi escolhido por representar a principal missão do projeto: **guiar decisões estratégicas** com base em dados confiáveis. Assim como uma bússola orienta o caminho em meio à incerteza, o projeto orienta as equipes na identificação de problemas, oportunidades e prioridades nos aplicativos, com base na percepção real dos usuários.

---

# 2. Arquitetura da Solução (Visão Geral)

A arquitetura proposta é baseada em um ambiente **Azure Cloud**, utilizando tecnologias para ingestão, processamento, armazenamento e visualização de dados. A solução é composta por várias camadas, cada uma com um papel específico no fluxo de dados.

![<arquitetura-data-master-compass>](https://github.com/gacarvalho/compass-deployment-cloud/blob/main/img/arqutietura.png?raw=true)

| **Camada / Componente** | **Descrição** | **Tecnologias / Versões** |
|:--------------------------|:----------------|:----------------------------|
| **Ingestão** | Extração de dados das fontes externas (Google Play e Apple Store) e internas (MongoDB) para o Data Lake. | **Azure Data Factory**, **Python orquestrado via Airflow** |
| **Armazenamento** | Estruturado em camadas **Raw**, **Bronze**, **Silver** e **Gold** para persistência. | **Azure Data Lake Storage (Gen2)** |
| **Banco de Dados Operacional** | Armazena as avaliações internas dos usuários. | **MongoDB 7** |
| **Processamento** | Executa pipelines distribuídos de transformação e agregação de dados. | **Azure Databricks (Apache Spark 3.5.0)** |
| **Telemetria e Observabilidade** | Centraliza métricas de desempenho e logs de execução. | **Grafana**, **Azure Log Analytics** |
| **Governança** | Gerencia catálogos de dados, metadados e políticas de acesso. | **Unity Catalog** |
| **Fontes Externas** | Captura avaliações e metadados de aplicativos das lojas. | **Google Play**, **Apple Store** |
| **Orquestração de Pipelines** | Coordena agendamentos, dependências e execução de tarefas de ETL/ELT. | **Airflow** |

---

# 3. Visão Geral da Arquitetura Técnica

O projeto Compass utiliza recursos no **Azure Cloud**, divididos em camadas de arquitetura **Batch** para *big data* e serviços de observabilidade.

| **Arquitetura** | **Camada** | **Descrição** | **Público alvo** |
|:----------------|:------------------------------|:------------------------------------------------------------------------------------------------|:-------------------------|
| Batch | Camada de Observabilidade | Serviços para coletar e monitorar dados de telemetria, fornecendo visibilidade sobre o desempenho e a integridade. | Time Dev, Sustentação |
| Batch | Camada de Aplicações | Aplicações desenvolvidas em PySpark (Python), com artefatos em *containers*, oferecendo processamento de dados escalável e modular. | Time Dev |

## 3.1 Descrição do Fluxo de Dados

O fluxo de dados é dividido em: Extração de Dados, Transformação de Dados e Carga de Dados.

> [!IMPORTANT]
> O *case* foi estruturado para ser aplicado em qualquer organização que deseje transformar dados em decisões mais estratégicas e orientadas. A solução é flexível e escalável. A **INSTITUIÇÃO** é utilizada como exemplo genérico.

### 3.1.1 Fonte (Datasource) de Dados

As fontes de dados são divididas entre internas (MongoDB) e externas (APIs de lojas de aplicativos).

| Fonte | Tipo | Detalhes | Observação |
|:---|:---|:---|:---|
| **Base Interna (MongoDB)** | Reviews da INSTITUIÇÃO | Coleção `Reviews INSTITUICAO`: Reviews de todas as aplicações da instituição. | Simulação via `dag_e_pipeline_compass_reviews` no Airflow para alimentar a coleção. |
| **Externo (Apple Store)** | API iTunes | `itunes.apple.com`: API utilizada para coletar avaliações da **Apple Store**. | Limitação de 500 avaliações mais recentes. |

### 3.1.2 Camada de Processamento

A Camada de Processamento é essencial para tratar, transformar e estruturar os dados. É implementada com **Apache Spark** em três estágios:

| Estágio | Tecnologia | Descrição |
|:---|:---|:---|
| **Spark Bronze – Ingestão** | Apache Spark | Realiza a ingestão dos dados brutos da camada `raw` e os leva para a camada Bronze. |
| **Spark Silver – Processamento Intermediário** | Apache Spark | Armazena e processa dados com histórico, aplicando transformações de limpeza, padronização e qualidade. |
| **Spark Gold – Enriquecimento e Agregação** | Apache Spark | Responsável por agregar e enriquecer os dados tratados, gerando visões analíticas valiosas. |

### 3.1.3 Camada de Armazenamento

A Camada de Armazenamento mantém os dados persistidos, garantindo **segurança, rastreabilidade, desempenho e organização**.

| Sistema | Propósito | Tecnologias Chave |
|:---|:---|:---|
| **MongoDB** | Armazenamento de dados funcionais estruturados (fonte de origem das avaliações internas). | Banco de dados NoSQL. |
| **Data Lake (ADLS Gen2)** | Armazenamento distribuído e escalável em diferentes níveis (Raw, Bronze, Silver, Gold). | **Azure Data Lake Storage (Gen2)** e **Delta Lake**. |
| **Log Analytics** | Centraliza logs e métricas de execução para monitoramento em tempo real. | **Azure Log Analytics**. |

#### Estrutura de Contêineres no Data Lake (ADLS Gen2)

| **Contêiner** | **Camada** | **Descrição** | **Tipo de Dado** |
|:----------------|:----------------|:------------------|:------------------|
| `raw-compass` | Raw | Área de aterrissagem **bruta** dos dados ingeridos. | Dados brutos |
| `b-compass` | Bronze | Dados que passam por **normalização e padronização inicial**. | Dados tratados |
| `s-compass` | Silver | Onde ocorre a **transformação e integração entre fontes**. | Dados refinados |
| `g-compass` | Gold | Dados **agregados e prontos para consumo analítico**. | Dados analíticos |
| `system-compass` | System | Armazena metadados da tabela de controle no Delta Lake. | Metadados Pipeline |

> **Observação:** Todas as camadas Delta suportam **Controle de versões, time travel, leituras ACID seguras** e **Auditoria**.

### 3.1.4 Camada de Visualização e Telemetria (Observabilidade)

A observabilidade é garantida por ferramentas que monitoram o desempenho e a saúde dos pipelines.

| Ferramenta | Objetivo Principal | Uso no Projeto Compass |
|:---|:---|:---|
| **Grafana** | Monitoramento e visualização de métricas operacionais e técnicas em tempo real. | Criação de dashboards para acompanhar KPIs técnicos e operacionais, integrando dados do Log Analytics. |
| **Log Analytics** | Centralização de logs e métricas de execução do Databricks e Data Factory. | Auditoria, diagnóstico e acompanhamento da saúde operacional dos pipelines. |

## 3.2 Aspectos Técnicos do Projeto Compass

O projeto foi concebido para execução em um ambiente *on-premises* com integração ao **Azure Cloud**.

### 3.2.1 Características da Execução do Projeto

O projeto é executado em uma infraestrutura **on-premises** onde os serviços são instanciados em **contêineres Docker**, orquestrados pelo **Docker Swarm** e toda parte de armazenamento do Delta Lake e processamento é feito na Azure Cloud.

> **Docker Swarm:** Escolhido por sua **simplicidade operacional**, **integração nativa com Docker** e **baixa sobrecarga computacional**, ideal para o ambiente local.

### 3.2.2.1 Infraestrutura do Projeto Compass (Azure)

Esta seção descreve a infraestrutura atual baseada em serviços **gerenciados da Azure**.

| Camada / Serviço | Componente | Descrição | Tecnologia / Serviço | Configuração Principal |
|:---|:---|:---|:---|:---|
| **Orquestração** | **Apache Airflow** | Agendar e orquestrar os pipelines de ingestão e transformação (ambiente local). | Airflow + Docker Compose | Integração com jobs Databricks. |
| **Camada Raw** | **Raw Layer** | Armazena dados brutos, como recebidos das fontes. | Azure Data Lake Gen2 | Formato **JSON**. |
| **Camada Bronze** | **Bronze Layer** | Dados estruturados e padronizados conforme *schema* de origem. | Delta Lake (ADLS) | Persistência com versionamento Delta. |
| **Camada Silver** | **Silver Layer** | Dados tratados, limpos e validados. | Delta Lake (ADLS) | Otimização com Z-Order. |
| **Camada Gold** | **Gold Layer** | Dados agregados e prontos para consumo analítico. | Delta Lake (ADLS) | Snapshot automático Delta. |
| **Banco Operacional** | **MongoDB** | Armazena dados de reviews (on-premises). | MongoDB (On-premisses) | Acesso via credenciais seguras. |
| **Processamento** | **Azure Databricks** | Executa os pipelines Spark de transformação e agregação. | Databricks Runtime 16.x / Spark 3.5 | Cluster. |
| **Observabilidade** | **Grafana / Log Analytics** | Monitoramento de métricas, logs e alertas dos pipelines. | Grafana Cloud + Azure Monitor | Painéis técnicos e de desempenho. |

💡 **Resumo das camadas Delta:**
- **Raw** → Dados brutos.
- **Bronze** → Dados estruturados.
- **Silver** → Dados tratados.
- **Gold** → Dados analíticos e agregados.

### 3.2.2.2 Aplicações do Projeto Compass (Batch)

As aplicações são desenvolvidas em **Apache Spark (PySpark)**, focadas em arquitetura **Batch** para garantir alta confiabilidade, escalabilidade e eficiência no processamento diário de grandes volumes de dados.

### 3.2.2.3 Pipeline do Projeto Compass (Airflow)

A orquestração é realizada pelo **Apache Airflow**. Cada DAG (Directed Acyclic Graph) representa um pipeline específico de negócio.

#### Resumo das DAGs do Projeto Compass

| **Nome da DAG** | **Descrição** | **Principais JOBs / Tarefas** |
|:---|:---|:---|
| **`DAG_COMPASS_PIPELINE`** | Pipeline **diária principal**. Realiza ingestão (Apple Store/MongoDB), processamento distribuído (Databricks) e disponibilização nas camadas **Raw, Bronze, Silver e Gold**. | Ingestão, Processamento Bronze, Silver (integração/enriquecimento) e Gold (agregação). |
| **`DAG_E_COMPASS_LOAD_EVENTS_MONGODB`** | Pipeline **eventual/sob demanda** para **gerar e inserir dados simulados (fake feedbacks)** no MongoDB. | Inserção de *feedbacks* fictícios para apoio na apresentação do *case*. |
| **`DAG_E_CREATE_TABLE_COMPASS`** | Pipeline **auxiliar de governança** para **criação e registro de tabelas no Databricks**, assegurando que as estruturas Delta estejam disponíveis. | Execução automática de jobs Databricks para criação de tabelas Delta. |

---

# 4. Fluxo Funcional e Jornada do Cliente

A solução foi projetada para atender o time de negócio, proporcionando uma visão estratégica das principais dores dos clientes e da concorrência. Permite análises em diferentes níveis de granularidade.

![<fluxo-funcional>](https://github.com/gacarvalho/compass-deployment-cloud/blob/main/img/jornada-cliente.png?raw=true)

A unificação e o enriquecimento das dores dos clientes com dados externos (Apple Store, Google Play, Reclame Aqui, etc.) permite uma visão mais holística, possibilitando:
* Identificação mais precisa de pontos de fricção.
* Priorização de melhorias baseada na percepção real dos usuários.
* Antecipação de problemas recorrentes.
* Alinhamento estratégico do time de Produto.
* Monitoramento contínuo da reputação da marca.

---

# 5. Compass como Produto Analytics para a Instituição

O projeto Compass tem como objetivo fornecer uma solução robusta e escalável, utilizando Engenharia de Dados para identificar as principais necessidades e desafios dos clientes, com potencial de expandir a análise para concorrentes.

## 5.1 Regras de Negócio

As regras funcionais implementadas garantem a estrutura final dos dados e a integridade mínima, focando no tratamento de dados semi-estruturados (comentários, avaliações, etc.).

### 🏷️ Regras de Negócios - Ingestão Bronze

| **ID** | **Regra de Negócio** | **Descrição Simplificada** | **Objetivo** |
|:---:|:---:|:---|:---|
| **RN001** | Configuração de Autenticação | Configura o Spark para usar **SAS Token** para acesso seguro ao Data Lake (**Raw**). | Permitir leitura de dados brutos usando credenciais seguras. |
| **RN002** | Otimizações Delta Lake | Define `optimizeWrite` e `autoCompact` como `true`. | Melhorar o desempenho de escrita e leitura na tabela Delta (Bronze). |
| **RN003** | Carregamento de Configuração | Lê a tabela `metadata_compass.data_params` (tabela de controle). | Obter metadados de ingestão dinamicamente (schemas e regras). |
| **RN006** | Inclusão de Colunas de Controle | Adiciona **`ingestion_ts`** e **`date_load`**. | Fornecer metadados essenciais para rastreabilidade, auditoria e particionamento. |
| **RN007** | Mapeamento e Cast de Colunas | Aplica o mapeamento (`schema_depara`) e realiza o *casting* para o tipo definido no `schema_target`. | Assegurar que os dados se conformem à estrutura e tipos de dados da tabela de destino. |
| **RN008** | Validação de Qualidade | Verifica **duplicidade** (baseada na `primary_key`) e **campos nulos** (`null_issue`). | Medir e reportar a qualidade dos dados antes da persistência. |
| **RN011** | Envio de Métricas | Envia um *payload* JSON com métricas detalhadas de execução para o **Azure Log Analytics**. | Garantir a observabilidade e rastreabilidade da execução. |

### 🏷️ Regras de Negócios - Processamento Silver de Reviews

| **ID** | **Regra de Negócio** | **Descrição Simplificada** | **Objetivo** |
|:---:|:---:|:---|:---|
| **RN003** | Leitura com Janela Temporal | Lê tabelas da Bronze (`apple_reviews` e `internal_db`) aplicando um filtro de partição com *`days_back`* (default: 365 dias). | Buscar dados recentes e históricos relevantes. |
| **RN004** | Padronização de Chave (Apple) | `review_id` da Apple é padronizado e gerado por **SHA2** (anonimização/hashing). | Criar um *schema* unificado e anonimizar chaves. |
| **RN005** | Padronização de Chave e Rating (Internos) | `review_id` gerado por **SHA2** (`client_identification` + `app_reference`). `feedback_rating` nulo padronizado para **0**. | Garantir unicidade da chave e consistência do *rating*. |
| **RN006** | União de Dados | Combina os *DataFrames* padronizados da Apple e Internal usando `unionByName`. | Integrar todas as fontes de reviews em um único *DataFrame*. |
| **RN007** | Remoção de Duplicatas | Remove duplicatas baseadas na chave composta (ID do review, ID do cliente, sistema de origem e referência do app). | Garantir a unicidade dos registros. |
| **RN010** | Remoção de Acentos | Aplica `translate` para remover acentos dos campos de texto. | Uniformizar dados textuais para análises e buscas. |
| **RN011** | Remoção de Emojis e Símbolos | Usa `regexp_replace` para remover emojis e símbolos não textuais. | Assegurar a qualidade e formatação dos textos. |
| **RN014** | Gravação e Sobrescrita | Grava na tabela de destino (`silver.<application>`) particionada por **`date_load`** (formato **YYYYMM**), usando `replaceWhere` para sobrescrever **apenas** a partição mensal atual. | Garantir a atomicidade e a gestão da partição mensal. |

### 🏷️ Regras de Negócios - Agregação e Métricas Gold

| **ID** | **Regra de Negócio** | **Descrição Simplificada** | **Objetivo** |
|:---:|:---:|:---|:---|
| **RN004** | Criação de Partição Temporal | Cria a coluna de partição **`review_month`** (formato **YYYYMM**) a partir de `review_date`. | Preparar a chave de partição para a tabela Gold. |
| **RN005** | Agregação e Sentimento | Agrupa os dados e calcula: **`total_reviews`**, **`positive_reviews`** (rating $\ge 4$), **`negative_reviews`** (rating $\le 2$) e **`neutral_reviews`** (rating $= 3$). | Criar as métricas de volume de reviews por mês e categoria de sentimento. |
| **RN006** | Cálculo de Média e Score de Sentimento | Calcula a `avg-rating` e o `sentiment-score` usando a fórmula: **$(\text{positive-reviews} - \text{negative-reviews}) / \text{total-reviews}$**. | Gerar Indicadores-Chave de Performance (KPIs) de sentimento. |
| **RN007** | Gravação e Sobrescrita | Grava na tabela de destino (`gold.<table_target>`) particionada por **`review_month`**, usando `replaceWhere` para sobrescrever a partição mensal atual. | Garantir a atomicidade e a gestão da partição mensal. |
| **RN008** | Otimização da Tabela | Executa o comando **`OPTIMIZE`** na tabela de destino. | Otimizar o desempenho de consulta na camada final. |

## 5.2 Dicionário de Dados

### `b_compass.apple_reviews` (Bronze)

**Descrição:** Armazena dados brutos dos *reviews* da Apple Store após a ingestão.
**Origem:** Apple Store. **Particionamento:** `date_load` (Partição diária, `YYYY-MM-DD`).

| Campo | Tipo de Dado | Descrição |
|:---|:---|:---|
| `author_ura` | `string` | URL do perfil do autor da avaliação. |
| `rating` | `int` | Nota atribuída pelo usuário (1 a 5). |
| `review_id` | `string` | Identificador único da avaliação na fonte. **Será *hashed* na Silver.** |
| `title` | `string` | Título dado ao *review*. |
| `content` | `string` | Conteúdo completo do *review* (texto principal). |
| **`date_load`** | `string` | **Data da carga do dado na partição.** |
| **`app_reference`**| `string` | **Referência do aplicativo.** |

### `b_compass.internal_db` (Bronze)

**Descrição:** Armazena dados brutos de *feedbacks* originados de **sistemas internos** da instituição.
**Origem:** Sistemas de Feedback Internos (CRM, SAC, etc.). **Particionamento:** `date_load` (Partição diária, `YYYY-MM-DD`).

| Campo | Tipo de Dado | Descrição |
|:---|:---|:---|
| `submission_date` | `timestamp` | Data e hora exata do envio do *feedback*. |
| `client_identification`| `string` | **Identificação do cliente (CPF ou CNPJ). Será *hashed* na Silver.** |
| `feedback_rating` | `int` | Nota atribuída pelo cliente (1 a 5). |
| `feedback_comment` | `string` | Comentário textual geral. |
| `service_type` | `string` | Tipo do serviço ou produto relacionado ao *feedback*. **Dimensão essencial.** |
| `source_channel` | `string` | Canal de origem do *feedback* (e.g., `website`, `mobile_app`). |
| **`date_load`** | `string` | **Data da carga do dado na partição.** |

### `s_compass.instituicao_reviews` (Silver)

**Descrição:** Tabela analítica unificada com todos os *reviews* (internos e externos) após padronização, limpeza e anonimização. **Única Fonte da Verdade (SSOT)**.
**Origem:** Unificação de `b_compass.apple_reviews` e `b_compass.internal_db`. **Particionamento:** `date_load` (Partição mensal, `YYYYMM`).

| Campo | Tipo de Dado | Descrição |
|:---|:---|:---|
| **`review_id`** | `string` | **Chave única do registro, gerada via SHA2 (anonimizada).** |
| **`client_id`** | `string` | **Identificador anonimizado do cliente.** |
| `review_date` | `timestamp` | Data e hora em que a avaliação foi submetida. |
| `review_rating` | `int` | Classificação da avaliação, de 1 a 5 (validado). |
| `review_text` | `string` | Texto completo da avaliação, **limpo e pronto para NLP** (sem acentos/emojis). |
| **`source_system`** | `string` | **Sistema de origem do feedback (e.g., `APPLE_REVIEWS`, `INTERNALDB_REVIEWS`).** |
| `service_type` | `string` | Tipo de serviço/produto relacionado ao feedback. |
| **`date_load`** | `string` | **Data de carregamento na partição (YYYYMM).** |

### `g_compass.reviews_customer_compass` (Gold)

**Descrição:** Tabela de **Métricas Agregadas** (Gold). Consolida *reviews* da Silver em níveis estratégicos e calcula indicadores.
**Origem:** Tabela unificada `s_compass.instituicao_reviews` (Silver). **Particionamento:** `review_month` (Partição mensal, `YYYYMM`).

| Campo | Tipo de Dado | Descrição |
|:---|:---|:---|
| **`review_month`** | `string` | **Mês da avaliação (YYYYMM). Chave de Particionamento.** |
| **`service_type`** | `string` | **Tipo de serviço ou produto relacionado ao feedback.** |
| **`app_reference`**| `string` | **Referência única do aplicativo.** |
| **`review_count`** | `bigint` | **Número total de reviews agregados no mês.** |
| **`average_rating`**| `double` | **Média aritmética das notas no período.** |
| **`nps_score`** | `double` | **Score de Sentimento (ou NPS Score).** |

### `metadata_compass.data_params` (Controle)

**Descrição:** Tabela de **Controle e Metadados** utilizada pelos pipelines para carregar dinamicamente as regras, schemas e configurações de origem/destino. Garante que os pipelines sejam **genéricos**.

| Campo | Tipo de Dado | Descrição |
|:---|:---|:---|
| `source_layer` | `string` | Camada de origem dos dados que o pipeline está lendo (e.g., `raw`, `silver`). |
| `table_name_target`| `string` | Nome da tabela de destino que será criada/atualizada. |
| **`schema_target`** | `array<struct>` | **Schema final da tabela de destino, incluindo tipos de dados e colunas de controle.** |
| **`schema_depara`** | `array<struct>` | **Mapeamento entre o nome da coluna de origem e o nome da coluna de destino.** |
| **`rule_control`** | `array<struct>` | **Regras de validação de qualidade e controle de pipeline (e.g., `not_empty`).** |


---


# 6. Melhorias e Considerações Finais

O projeto Compass reforça o papel da Engenharia de Dados como elemento central na construção de soluções voltadas para o negócio e para a experiência do usuário. Ao oferecer uma estrutura confiável, escalável e orientada à geração de *insights*, a iniciativa empodera times de produto com dados relevantes sobre seus próprios aplicativos e fornece uma base comparativa frente aos concorrentes.

Com isso, o Compass se torna uma ferramenta valiosa para instituições que buscam não só entender, mas também **antecipar as necessidades dos seus clientes** — fortalecendo sua presença no mercado e avançando na jornada rumo à principalidade financeira.