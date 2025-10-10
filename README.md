🧭 ♨️ COMPASS: Solução de Analytics para Experiência do Cliente

---

<p align="left">
  <img src="https://img.shields.io/badge/projeto-Compass-blue?style=flat-square" alt="Projeto">
  <img src="https://img.shields.io/badge/status-deployed-green?style=flat-square" alt="Status">
  <img src="https://img.shields.io/badge/autor-Gabriel_Carvalho-lightgrey?style=flat-square" alt="Autor">
</p>

O repositório **compass-deployment-cloud** é uma solução desenvolvida no contexto do programa **Data Master**, promovido pela F1rst Tecnologia. Seu objetivo é disponibilizar uma plataforma robusta e escalável para **captura, processamento e análise de feedbacks de clientes**.

![<data-master-compass>](https://github.com/gacarvalho/repo-spark-delta-iceberg/blob/main/header.png?raw=true)

Este documento apresenta a visão geral do case, abrangendo desde os objetivos iniciais até a descrição técnica da arquitetura, fluxos funcionais, tecnologias empregadas, instruções para execução e considerações finais. A proposta é oferecer um panorama completo sobre o funcionamento do Compass como **produto de analytics voltado à experiência do cliente**.

---

1.  [**Objetivo do Case**](#1-objetivo-do-case)
    * [1.1 O Case Compass](#11-o-case-compass)
2.  [**Arquitetura da Solução (Visão Geral)**](#2-arquitetura-da-solução-visão-geral)
3.  [**Visão Geral da Arquitetura Técnica**](#3-visão-geral-da-arquitetura-técnica)
    * [3.1 Descrição do Fluxo de Dados](#31-descrição-do-fluxo-de-dados)
        * [3.1.1 Fonte (Datasource) de Dados](#311-fonte-datasource-de-dados)
        * [3.1.2 Camada de Processamento](#312-camada-de-processamento)
        * [3.1.3 Camada de Armazenamento](#313-camada-de-armazenamento)
        * [3.1.4 Camada de Visualização e Telemetria (Observabilidade)](#314-camada-de-visualização-e-telemetria-observabilidade)
    * [3.2 Aspectos Técnicos do case Compass](#32-aspectos-técnicos-do-case-compass)
        * [3.2.1 Tecnologias Utilizadas](#321-tecnologias-utilizadas)
        * [3.2.2 Características da Execução do case](#322-características-da-execução-do-case)
        * [3.2.2.1 Infraestrutura do case Compass (Azure)](#3221-infraestrutura-do-case-compass-azure)
        * [3.2.2.2 Aplicações do case Compass (Batch)](#3222-aplicações-do-case-compass-batch)
        * [3.2.2.3 Pipeline do case Compass (Airflow)](#3223-pipeline-do-case-compass-airflow)
4.  [**Fluxo Funcional e Jornada do Cliente**](#4-fluxo-funcional-e-jornada-do-cliente)
5.  [**Compass como Produto Analytics para a Instituição**](#5-compass-como-produto-analytics-para-a-instituição)
    * [5.1 Regras](#51-regras)
    * [5.2 Dicionário de Dados](#52-dicionário-de-dados)
        * [`b_compass.apple_reviews` (Bronze)](#b_compassapple_reviews-bronze)
        * [`b_compass.internal_db` (Bronze)](#b_compassinternal_db-bronze)
        * [`s_compass.instituicao_reviews` (Silver)](#s_compassinstituicao_reviews-silver)
        * [`g_compass.reviews_customer_compass` (Gold)](#g_compassreviews_customer_compass-gold)
        * [`metadata_compass.data_params` (Controle)](#metadata_compassdata_params-controle)
6. [**Custo do case**](#6-custo-do-case)
7. [**Instruções para Configuração e Execução do case Compass**](#7-instruções-para-configuração-e-execução-do-case-compass)
8. [**Melhorias e Considerações Finais**](#7-melhorias-e-considerações-finais)
    * [8.1 Melhorias do case](#81-melhorias-do-case)
    * [8.2 Melhorias e Considerações Finais](#82-melhorias-e-considerações-finais)
9. [**Referências**](#9-referências)

---


# 1. Objetivo do Case

A idealização deste case surgiu da necessidade de fortalecer o alinhamento entre o time de negócios e a Engenharia de Dados, com foco na resolução de desafios práticos relacionados à **jornada do usuário**. A iniciativa teve como ponto de partida a ausência de visibilidade aprofundada sobre a forma como os clientes interagem com os produtos e serviços da empresa.

Diante desse cenário, o objetivo central foi desenvolver uma solução capaz de **capturar, tratar e estruturar dados de interação dos usuários**, viabilizando análises confiáveis e acionáveis para suporte à tomada de decisão. A arquitetura foi desenhada com foco em flexibilidade e escalabilidade, permitindo sua aplicação em diferentes contextos e ampliando o potencial de geração de valor, inclusive para comparação com padrões comportamentais de outras empresas do setor.

## 1.1 O Case Compass

O Case **Data Master Compass** é uma iniciativa de Engenharia de Dados projetada para capturar e analisar **feedbacks de clientes** sobre produtos e serviços. O nome **Compass** reflete seu propósito: **orientar** o time de negócios na melhoria contínua de processos e soluções, com base em dados reais.

A solução centraliza as informações em um **Data Lake no ambiente Cloud Azure**, organizando os dados por data de referência e segmento de público. Isso proporciona *insights* valiosos para **Product Owners, Product Managers e Gerentes de Projetos**, permitindo decisões baseadas em evidências e alinhadas às necessidades reais dos usuários.

> [!NOTE]
> 🧭 **Por que o nome "Compass"?**
> O nome Compass (em português, bússola) foi escolhido por representar a principal missão do case: **guiar decisões estratégicas** com base em dados confiáveis. Assim como uma bússola orienta o caminho em meio à incerteza, o case orienta as equipes na identificação de problemas, oportunidades e prioridades nos aplicativos, com base na percepção real dos usuários.

---

# 2. Arquitetura da Solução (Visão Geral)

A arquitetura proposta é baseada em um ambiente **Azure Cloud**, utilizando tecnologias para ingestão, processamento, armazenamento e visualização de dados. A solução é composta por várias camadas, cada uma com um papel específico no fluxo de dados.

![<arquitetura-data-master-compass>](https://github.com/gacarvalho/compass-deployment-cloud/blob/main/img/arqutietura.png?raw=true)

| **Camada / Componente** | **Descrição** | **Tecnologias / Versões** |
|:--------------------------|:----------------|:----------------------------|
| **Ingestão** | Extração de dados das fontes externas (Apple Store) e internas (MongoDB) para o Data Lake. | **Azure Data Factory**, **Python orquestrado via Airflow** |
| **Armazenamento** | Estruturado em camadas **Raw**, **Bronze**, **Silver** e **Gold** para persistência. | **Azure Data Lake Storage (Gen2)** |
| **Banco de Dados Operacional** | Armazena as avaliações internas dos usuários. | **MongoDB 7** |
| **Processamento** | Executa pipelines distribuídos de transformação e agregação de dados. | **Azure Databricks (Apache Spark 3.5.0)** |
| **Telemetria e Observabilidade** | Centraliza métricas de desempenho e logs de execução. | **Grafana**, **Azure Log Analytics** |
| **Governança** | Gerencia catálogos de dados, metadados e políticas de acesso. | **Unity Catalog** |
| **Fontes Externas** | Captura avaliações e metadados de aplicativos das lojas. | ****Apple Store** |
| **Orquestração de Pipelines** | Coordena agendamentos, dependências e execução de tarefas de ETL/ELT. | **Airflow** |

---

# 3. Visão Geral da Arquitetura Técnica

O case Compass utiliza recursos no **Azure Cloud**, divididos em camadas de arquitetura **Batch** para *big data* e serviços de observabilidade.

| **Arquitetura** | **Camada** | **Descrição** | **Público alvo**                                                   |
|:----------------|:------------------------------|:------------------------------------------------------------------------------------------------|:-------------------------------------------------------------------|
| Batch | Camada de Observabilidade | Serviços para coletar e monitorar dados de telemetria, fornecendo visibilidade sobre o desempenho e a integridade. | Time Dev, Pessoa responsável em sustentar a aplicação em produção. |
| Batch | Camada de Aplicações | Aplicações desenvolvidas em PySpark (Python), com artefatos em *containers*, oferecendo processamento de dados escalável e modular. | Time Dev                                                           |

## 3.1 Descrição do Fluxo de Dados

O fluxo de dados é dividido em: Extração de Dados, Transformação de Dados e Carga de Dados.

> [!IMPORTANT]
> O *case* foi estruturado para ser aplicado em qualquer organização que deseje transformar dados em decisões mais estratégicas e orientadas. A solução é flexível e escalável. A **INSTITUIÇÃO** é utilizada como exemplo genérico.

### 3.1.1 Fonte (Datasource) de Dados

As fontes de dados são divididas entre internas (MongoDB) e externas (APIs de lojas de aplicativos).

| Fonte | Tipo | Detalhes | Observação |
|:---|:---|:---|:---|
| **Base Interna (MongoDB)** | Reviews da INSTITUIÇÃO | Coleção `Reviews INSTITUICAO`: Reviews de todas as aplicações da instituição. | Simulação via `DAG_COMPASS_PIPELINE` no Airflow para alimentar a coleção. |
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

| Ferramenta | Objetivo Principal | Uso no Case Compass |
|:---|:---|:---|
| **Grafana** | Monitoramento e visualização de métricas operacionais e técnicas em tempo real. | Criação de dashboards para acompanhar KPIs técnicos e operacionais, integrando dados do Log Analytics. |
| **Log Analytics** | Centralização de logs e métricas de execução do Databricks e Data Factory. | Auditoria, diagnóstico e acompanhamento da saúde operacional dos pipelines. |

O dashboard contempla 

## 3.2 Aspectos Técnicos do Case Compass

O Case foi concebido para execução em um ambiente *on-premises* com integração ao **Azure Cloud**.

### 3.2.1 Características da Execução do Case

O Case é executado em uma infraestrutura **on-premises** onde os serviços são instanciados em **contêineres Docker**, orquestrados pelo **Docker Swarm** e toda parte de armazenamento do Delta Lake e processamento é feito na Azure Cloud.

> **Docker Swarm:** Escolhido por sua **simplicidade operacional**, **integração nativa com Docker** e **baixa sobrecarga computacional**, ideal para o ambiente local.

### 3.2.2.1 Infraestrutura do Case Compass (Azure)

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

### 3.2.2.2 Aplicações do Case Compass (Batch)

As aplicações são desenvolvidas em **Apache Spark (PySpark)**, focadas em arquitetura **Batch** para garantir alta confiabilidade, escalabilidade e eficiência no processamento diário de grandes volumes de dados.

### 3.2.2.3 Pipeline do Case Compass (Airflow)

A orquestração é realizada pelo **Apache Airflow**. Cada DAG (Directed Acyclic Graph) representa um pipeline específico de negócio.

#### Resumo das DAGs do Case Compass

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

O Case Compass tem como objetivo fornecer uma solução robusta e escalável, utilizando Engenharia de Dados para identificar as principais necessidades e desafios dos clientes, com potencial de expandir a análise para concorrentes.

## 5.1 Regras

As regras funcionais implementadas garantem a estrutura final dos dados e a integridade mínima, focando no tratamento de dados semi-estruturados (comentários, avaliações, etc.).

### 🏷️ Regras - Ingestão Bronze

| **ID** |            **Regra**            | **Descrição Simplificada** | **Objetivo** |
|:---:|:-------------------------------:|:---|:---|
| **RN001** |  Configuração de Autenticação   | Configura o Spark para usar **SAS Token** para acesso seguro ao Data Lake (**Raw**). | Permitir leitura de dados brutos usando credenciais seguras. |
| **RN002** |     Otimizações Delta Lake      | Define `optimizeWrite` e `autoCompact` como `true`. | Melhorar o desempenho de escrita e leitura na tabela Delta (Bronze). |
| **RN003** |  Carregamento de Configuração   | Lê a tabela `metadata_compass.data_params` (tabela de controle). | Obter metadados de ingestão dinamicamente (schemas e regras). |
| **RN006** | Inclusão de Colunas de Controle | Adiciona **`ingestion_ts`** e **`date_load`**. | Fornecer metadados essenciais para rastreabilidade, auditoria e particionamento. |
| **RN007** |  Mapeamento e Cast de Colunas   | Aplica o mapeamento (`schema_depara`) e realiza o *casting* para o tipo definido no `schema_target`. | Assegurar que os dados se conformem à estrutura e tipos de dados da tabela de destino. |
| **RN008** |     Validação de Qualidade      | Verifica **duplicidade** (baseada na `primary_key`) e **campos nulos** (`null_issue`). | Medir e reportar a qualidade dos dados antes da persistência. |
| **RN011** |        Envio de Métricas        | Envia um *payload* JSON com métricas detalhadas de execução para o **Azure Log Analytics**. | Garantir a observabilidade e rastreabilidade da execução. |

### 🏷️ Regras - Processamento Silver de Reviews

| **ID** |                 **Regra**                 | **Descrição Simplificada** | **Objetivo** |
|:---:|:-----------------------------------------:|:---|:---|
| **RN003** |        Leitura com Janela Temporal        | Lê tabelas da Bronze (`apple_reviews` e `internal_db`) aplicando um filtro de partição com *`days_back`* (default: 365 dias). | Buscar dados recentes e históricos relevantes. |
| **RN004** |       Padronização de Chave (Apple)       | `review_id` da Apple é padronizado e gerado por **SHA2** (anonimização/hashing). | Criar um *schema* unificado e anonimizar chaves. |
| **RN005** | Padronização de Chave e Rating (Internos) | `review_id` gerado por **SHA2** (`client_identification` + `app_reference`). `feedback_rating` nulo padronizado para **0**. | Garantir unicidade da chave e consistência do *rating*. |
| **RN006** |              União de Dados               | Combina os *DataFrames* padronizados da Apple e Internal usando `unionByName`. | Integrar todas as fontes de reviews em um único *DataFrame*. |
| **RN007** |           Remoção de Duplicatas           | Remove duplicatas baseadas na chave composta (ID do review, ID do cliente, sistema de origem e referência do app). | Garantir a unicidade dos registros. |
| **RN010** |            Remoção de Acentos             | Aplica `translate` para remover acentos dos campos de texto. | Uniformizar dados textuais para análises e buscas. |
| **RN011** |       Remoção de Emojis e Símbolos        | Usa `regexp_replace` para remover emojis e símbolos não textuais. | Assegurar a qualidade e formatação dos textos. |
| **RN014** |          Gravação e Sobrescrita           | Grava na tabela de destino (`silver.<application>`) particionada por **`date_load`** (formato **YYYYMM**), usando `replaceWhere` para sobrescrever **apenas** a partição mensal atual. | Garantir a atomicidade e a gestão da partição mensal. |

### 🏷️ Regras - Agregação e Métricas Gold

| **ID** |               **Regra**                | **Descrição Simplificada** | **Objetivo** |
|:---:|:--------------------------------------:|:---|:---|
| **RN004** |      Criação de Partição Temporal      | Cria a coluna de partição **`review_month`** (formato **YYYYMM**) a partir de `review_date`. | Preparar a chave de partição para a tabela Gold. |
| **RN005** |         Agregação e Sentimento         | Agrupa os dados e calcula: **`total_reviews`**, **`positive_reviews`** (rating $\ge 4$), **`negative_reviews`** (rating $\le 2$) e **`neutral_reviews`** (rating $= 3$). | Criar as métricas de volume de reviews por mês e categoria de sentimento. |
| **RN006** | Cálculo de Média e Score de Sentimento | Calcula a `avg-rating` e o `sentiment-score` usando a fórmula: **$(\text{positive-reviews} - \text{negative-reviews}) / \text{total-reviews}$**. | Gerar Indicadores-Chave de Performance (KPIs) de sentimento. |
| **RN007** |         Gravação e Sobrescrita         | Grava na tabela de destino (`gold.<table_target>`) particionada por **`review_month`**, usando `replaceWhere` para sobrescrever a partição mensal atual. | Garantir a atomicidade e a gestão da partição mensal. |
| **RN008** |          Otimização da Tabela          | Executa o comando **`OPTIMIZE`** na tabela de destino. | Otimizar o desempenho de consulta na camada final. |

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

🧭 Dashboard Técnico - Aplicações e Dashboard Técnico

Este dashboard foi desenvolvido para fornecer uma visão técnica consolidada da execução dos pipelines no Case Compass, permitindo o monitoramento contínuo da saúde operacional, da qualidade dos dados e identificação de falhas dos processos em produção.

📌 O que você encontrará neste painel:

- Status geral do pipeline: identificação clara de execuções bem-sucedidas ou com falhas.
- Volume de jobs executados, com detalhamento entre sucessos e falhas.
- Indicadores de qualidade de dados, incluindo:
- Presença de valores nulos;
- Inconsistências nos dados;
- Registros duplicados.
- Tempo médio de processamento separado por camada Bronze, Silver e Gold.

📌 Público-alvo

Este painel é direcionado a times técnicos de Engenharia de Dados, Pessoa responsável pela aplicação em produção e Operações, com o objetivo de garantir resposta ágil a incidentes, visibilidade total do processo e tomada de decisão baseada em evidências.

![<grafana>](https://github.com/gacarvalho/compass-deployment-cloud/blob/main/img/dashboard-grafana.png?raw=true)


---

# 6. Custo do Case

A **execução completa** do **pipeline** Compass, abrangendo as etapas de ingestão, processamento e armazenamento, teve um custo total de R$ 3,55.

> **Nota:** A execução completa do pipeline tem custo estimado de R$ 3,55, porém há custos adicionais relacionados à infraestrutura de rede, como o NAT Gateway e recursos de conectividade.


![<custos>](https://github.com/gacarvalho/compass-deployment-cloud/blob/main/img/custos.png?raw=true)

Um dos principais motivos para o case Compass ter adotado o MongoDB em ambiente on-premises, em vez do MongoDB Atlas na Azure Cloud, foi a redução de custos operacionais. Essa decisão permitiu evitar um gasto estimado de aproximadamente R$ 320,46 por mês se utilizassemos uma instância M10, mantendo a performance e a disponibilidade necessárias para o case.


| **Cluster** | **Configuração** | **vCPUs / RAM / Storage** | **Custo por hora (US$)** | **Custo mensal (US$)** | **Custo mensal (R$)**¹ |
|--------------|------------------|----------------------------|---------------------------|--------------------------|-------------------------|
| **M10** | Nível básico para workloads pequenas | 2 vCPUs / 2 GB RAM / 8 GB disco | **$0.08/h** | **$58.00/mês** | **≈ R$ 320/mês** |
| **M20** | Nível intermediário para workloads médias | 4 vCPUs / 4 GB RAM / 16 GB disco | **$0.20/h** | **$143.74/mês** | **≈ R$ 795/mês** |
| **M30** | Nível avançado para workloads maiores | 8 vCPUs / 8 GB RAM / 32 GB disco | **$0.54/h** | **$386.20/mês** | **≈ R$ 2.135/mês** |

¹ Conversão aproximada considerando **1 USD = R$ 5,53**.



---
# 7. Instruções para Configuração e Execução do Case Compass


## 7.1 Pré-requisitos
---
### Requisitos da Máquina Local
- **CPU:** Mínimo de 4 vCPUs
- **Memória RAM:** Mínimo 16 GB
- **Disco Rígido:** Mínimo 500GB
- **Sistema Operacional:** Linux (recomendado)

### Requisitos de Conectividade
- **Acesso à Internet:** Necessário para download de imagens, dependências e integração com APIs externas

### Portas Necessárias (Protocolos TCP)
Certifique-se de que as seguintes portas estejam **liberadas**:

| Porta  | Descrição / Serviço Relacionado                          |
|--------|----------------------------------------------------------|
| 8080	  | Apache Airflow Webserver (Interface Web de Orquestração) |
| 4000	  | Grafana (Visualização e Monitoramento/Observabilidade)   |
| 27017  | MongoDB (Banco de Dados Operacional)                    |

> **Nota:** Ajuste as portas personalizadas conforme sua stack.

### Ferramentas Necessárias
- **Git** – para clonar o repositório do Case
- **Docker e Docker Compose** – para orquestração dos serviços via containers e adicione o usuário atual ao grupo docker, o que permite que ele execute comandos Docker sem precisar usar sudo. `sudo usermod -aG docker $USER` e para ativar o comando sem reiniciar a maquina utilize `newgrp docker`
- **Acesso Root** – necessário para instalações, permissões e execução de containers com privilégios
- **Make** – para executar comandos definidos no Makefile que facilitam tarefas como build, deploy e testes


> [!NOTE]
> Certifique-se de atender **todos os requisitos mínimos**, especialmente os relacionados à **máquina local**. Eles são fundamentais para garantir o funcionamento adequado e o desempenho esperado do case.

---


## 7.2 Passos de configuração e execução do Case Compass
---

**Execução 1 - Replicação do case via repositório**

Clonagem do Repositório

Clone o repositório utilizando o comando abaixo ou acesse diretamente através do link: [compass-deployment](https://github.com/gacarvalho/compass-deployment-cloud)

```bash
git clone git@github.com:gacarvalho/compass-deployment-cloud.git
```

Inicialização do Docker Swarm

Dentro do diretório raiz do projeto `compass-deployment-cloud`, inicialize o Docker Swarm com o seguinte comando:

```bash
docker swarm init --advertise-addr <ip-xxx.xxx.x.x>
```
E agora substituir `<ip-xxx.xxx.x.x>` pelo o seu IP! 

O parâmetro **`--advertise-addr`** é essencial no comando `docker swarm init` e serve para **especificar qual endereço IP o nó Manager deve usar para se anunciar e se comunicar** com todos os outros Managers e Workers do cluster.

### Por Que Você Precisa Especificar o IP?

Quando você executa `docker swarm init --advertise-addr 192.168.0.x`, você está forçando o Manager a usar um IP específico, o que é crucial em dois cenários principais:

1.  **Ambiguidade de Rede (IPs Múltiplos):** Se a sua máquina tiver múltiplas interfaces de rede ativas (por exemplo, uma LAN, uma Wi-Fi e uma VPN), ela terá vários IPs. O Docker não consegue adivinhar qual é o correto para o cluster e **exige** que você especifique o endereço.
2.  **Comunicação Consistente (Protocolo Raft):** Este IP é usado pelo **Protocolo Raft** para a comunicação interna entre os nós Manager. Usar um IP fixo garante que, mesmo após reinicializações, os Managers consigam se localizar e manter o estado do cluster estável.

### Como Utilizar o Endereço IP

### 1. Descobrir o IP Acessível

Você precisa do endereço IP **real** do Manager, que deve ser alcançável por todos os outros nós na sua rede.

| Sistema Operacional | Comando | Dica |
|:--------------------| :--- | :--- |
| **Linux**           | `ip addr show`  | Procure o endereço `inet` na sua interface principal (`eth0`, `en0`, etc.). |

*Exemplo de IP encontrado: `192.168.0.10`*

### 2. O Comando Correto

Use o IP que você encontrou como valor para o parâmetro:

```bash
docker swarm init --advertise-addr 192.168.0.10
```

![<docker-swarm-1>](https://github.com/gacarvalho/compass-deployment-cloud/blob/main/img/passo-1.png?raw=true)

Criação da Rede Docker

A criação da rede será realizada via `Makefile`. Certifique-se de estar na raiz do repositório conforme o path abaixo:

> **Exemplo -  raiz do projeto**: `{path-projeto}/compass-deployment$`

Execute o comando a seguir:

```bash
make create-network
```

![<docker-swarm-2>](https://github.com/gacarvalho/compass-deployment-cloud/blob/main/img/passo-2.png?raw=true)

Para preparar o ambiente, execute o seguinte comando para criar a estrutura de diretórios necessária dentro de {projeto}/mnt:

```bash
# Cria o grupo 'airflow' (caso não exista) -> Necessário para executar o comando make prepare-mnt
sudo groupadd airflow

# Cria o usuário 'airflow', adiciona-o ao grupo 'airflow' e cria seu diretório home -> Necessário para executar o comando make prepare-mnt
sudo useradd -m -g airflow airflow

make prepare-mnt
```

O resultado esperado é algo semelhante ao log abaixo:

** No meu caso já esta criado, então vai dar esse output!

![<docker-swarm-3>](https://github.com/gacarvalho/compass-deployment-cloud/blob/main/img/passo-3.png?raw=true)


Configuração do Arquivo `.env`

Crie um arquivo de variáveis de ambiente no diretório indicado:

```bash
touch layer_batch/deployment/.env
```

Cole o conteúdo abaixo dentro do arquivo `.env`:


```env
MONGO_USER_ADMIN=gacarvalho
MONGO_PASS_ADMIN=santand@r
MONGO_USER=app_user
MONGO_PASS=santand@r
MONGO_HOST=mongodb
MONGO_PORT=27017
MONGO_DB=compass
AIRFLOW_IMAGE_NAME=apache/airflow:2.5.1
AIRFLOW_PROJ_DIR=../../mnt/airflow/
AIRFLOW_UID=50000
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
AIRFLOW_ENV_DIR=.
```

![<docker-swarm-4>](https://github.com/gacarvalho/compass-deployment-cloud/blob/main/img/passo-4.png?raw=true)

E faça uma cópia do arquivo `.env` para uma pasta que você deverá criar também em `/env` na raiz do computador!

```bash
sudo mkdir /env
cp layer_batch/deployment/.env /env/
```

**Deployment do  Airflow**
---

Antes de iniciar o deploy do Airflow, é essencial preparar o ambiente, garantindo que a estrutura de diretórios, permissões e usuários estejam corretamente configurados.

Deploy do Serviço Airflow via Makefile

Para realizar o deploy inicial do serviço Airflow, execute:

```bash
make deployment-airflow-service
```

Caso necessário, siga os ajustes descritos abaixo.

---

**Ajustes no Host**

Ajustar UID do Usuário `airflow`

Garante que o UID do usuário `airflow` no host corresponda ao UID do container (50000), evitando conflitos de permissões em volumes:

```bash
sudo usermod -u 50000 airflow
```

Criar Usuário e Grupo `airflow` (caso não existam)

```bash
sudo groupadd airflow
sudo useradd -r -m -g airflow airflow
```

**Ajustar Permissões dos Diretórios**

Definir o usuário e grupo corretos nas pastas de volumes montados:

```bash
sudo chown -R airflow:airflow mnt/airflow/
sudo chown -R airflow:airflow /opt/airflow/
```

**Ajustar Permissões no Diretório de Logs**

```bash
sudo chmod -R 755 /opt/airflow/logs
sudo mkdir -p /opt/airflow/logs/scheduler
```

Para acesso local (opcional, apenas durante desenvolvimento):

```bash
sudo chown -R $(whoami):$(whoami) /opt/airflow/logs
chmod -R 775 /opt/airflow/logs
sudo chown -R airflow:airflow /opt/airflow/logs
```

**Preparar Diretório de Plugins**

```bash
sudo mkdir -p mnt/airflow/plugins
sudo chmod -R 775 mnt/airflow/plugins/
```

---

**Verificação dos Volumes**

Liste os diretórios para garantir a estrutura correta:

```bash
ls -la /opt/airflow/
ls -la /opt/airflow/logs/
```

Certifique-se de que as permissões estejam corretas.

---

**🛠️ Inicialização e Ajuste do Banco de Dados**

Após os ajustes:

```bash
make deployment-airflow-service
```

Verifique se os serviços estão no ar:

```bash
docker service ls
```

Exemplo de retorno do comando:

```
ID             NAME                                   MODE         REPLICAS   IMAGE                  PORTS
psojstfep72o   deployment-airflow_airflow-cli         replicated   1/1        apache/airflow:2.8.1   
0dj15z7u9sl4   deployment-airflow_airflow-init        replicated   0/1        apache/airflow:2.8.1   
5nx5r42p5onu   deployment-airflow_airflow-scheduler   replicated   1/1        apache/airflow:2.8.1   
vucri1omfivc   deployment-airflow_airflow-triggerer   replicated   1/1        apache/airflow:2.8.1   
ud05mmm69cjt   deployment-airflow_airflow-webserver   replicated   1/1        apache/airflow:2.8.1   *:8080->8080/tcp
78wswdjp040u   deployment-airflow_airflow-worker      replicated   1/1        apache/airflow:2.8.1   
q611xj4ohqxa   deployment-airflow_flower              replicated   1/1        apache/airflow:2.8.1   *:5555->5555/tcp
jijfkrdzfh97   deployment-airflow_postgres            replicated   1/1        postgres:13            
9piznydj4vnc   deployment-airflow_redis               replicated   1/1        redis:latest               
```

**Correção de Erro de Inicialização do Banco de Dados**

> ⚠️ Caso o `airflow-webserver` exiba o erro: `ERROR: You need to initialize the database. Please run 'airflow db init'.`

Execute:

```bash
docker exec -it $(docker ps -q -f name=airflow-webserver) bash
airflow db init
airflow db migrate
exit
```

Depois, reimplantamos:

```bash
make deployment-airflow-service
```

---

**Criação do Usuário Admin**

Acesse a interface Web do Airflow:

```
http://<IP-OU-HOST>:8080/
```

Crie o usuário administrador:

```bash
docker exec -it $(docker ps -q -f name=airflow-webserver) airflow users create \
   --username admin \
   --firstname Admin \
   --lastname User \
   --role Admin \
   --email admin@example.com \
   --password admin
```

**Login:**

- **Usuário:** `admin`
- **Senha:** `admin`

Exemplo de visualização das DAGs:

![<docker-swarm-5>](https://github.com/gacarvalho/compass-deployment-cloud/blob/main/img/passo-5.png?raw=true)
![<docker-swarm-6>](https://github.com/gacarvalho/compass-deployment-cloud/blob/main/img/passo-6.png?raw=true)


**Deployment do Mongo DB**
---

Para subirmos o serviço do Mongo DB, será necessário executar o Makefile para deployarmos:

```bash
make deployment-mongodb-service
```

O resultado esperado com o comando `docker service ls` é que o scale do pod seja 1/1

```
ID             NAME                                   MODE         REPLICAS   IMAGE                  PORTS    
uoojqf4dijrl   deployment-mondodb_database-mongodb    replicated   1/1        mongo:7                *:27017->27017/tcp
```

Agora precisamos criar os usuários de serviço e as collections, primeiro será necessário entrar no terminal do container:

```bash
docker exec -it $(docker ps -q -f name=database-mongodb) mongosh admin
```

Agora, será necessário criar as collections com o comando abaixo:

```bash
use compass
db.createCollection('reviews_instituicao_compass')
```
![<docker-swarm-7>](https://github.com/gacarvalho/compass-deployment-cloud/blob/main/img/passo-7.png?raw=true)


Depois da criação das collections, será necessário também criar um usuário de "serviço", nesse caso estou usando como `gacarvalho`, mas voce pode alterar aqui e posteriormente no arquivo .env do projeto.

```bash
use compass
db.createUser({
  user: "gacarvalho",
  pwd: "santand@r",
  roles: [
    { role: "root", db: "admin" }
  ]
})

db.createUser({
  user: "app_user",
  pwd: "secure_password123",
  roles: [
    { role: "root", db: "admin" }
  ]
})
```

Logo após a criação do usuário, você poderá sair do container e testar o acesso do usuário novo criado pelo comando abaixo:

```bash
docker exec -it $(docker ps -q -f name=database-mongodb) mongosh "mongodb://gacarvalho:santand@r@localhost:27017/compass?authSource=compass"
```

![<docker-swarm-8>](https://github.com/gacarvalho/compass-deployment-cloud/blob/main/img/passo-8.png?raw=true)


**Deployment do Grafana**
---

Agora, vamos subir o serviço do Grafana pelo comando abaixo:

```bash
make deployment-grafana-service
```

Nesse momento o seu deployment já ficaria nesse status:

```
gacarvalho@gacarvalho-user:~/IdeaProjects/compass-deployment-cloud$ docker service ls
ID             NAME                                   MODE         REPLICAS   IMAGE                    PORTS
psojstfep72o   deployment-airflow_airflow-cli         replicated   1/1        apache/airflow:2.8.1     
0dj15z7u9sl4   deployment-airflow_airflow-init        replicated   0/1        apache/airflow:2.8.1     
5nx5r42p5onu   deployment-airflow_airflow-scheduler   replicated   1/1        apache/airflow:2.8.1     
vucri1omfivc   deployment-airflow_airflow-triggerer   replicated   1/1        apache/airflow:2.8.1     
ud05mmm69cjt   deployment-airflow_airflow-webserver   replicated   1/1        apache/airflow:2.8.1     *:8080->8080/tcp
78wswdjp040u   deployment-airflow_airflow-worker      replicated   1/1        apache/airflow:2.8.1     
q611xj4ohqxa   deployment-airflow_flower              replicated   1/1        apache/airflow:2.8.1     *:5555->5555/tcp
jijfkrdzfh97   deployment-airflow_postgres            replicated   1/1        postgres:13              
9piznydj4vnc   deployment-airflow_redis               replicated   1/1        redis:latest             
srnzw2eotl39   deployment-grafana_grafana             replicated   2/2        grafana/grafana:latest   *:4000->3000/tcp
uoojqf4dijrl   deployment-mondodb_database-mongodb    replicated   1/1        mongo:7                  *:27017->27017/tcp
```

![<docker-swarm-10>](https://github.com/gacarvalho/compass-deployment-cloud/blob/main/img/passo-10.png?raw=true)

📌 Dashboard e seu código para IMPORT

![<docker-swarm-11>](https://github.com/gacarvalho/compass-deployment-cloud/blob/main/img/passo-11.png?raw=true)

> [!NOTE]
> Lembrando que ao realizar o importe do JSON para o grafana voce precisa 1º => Criar uma conexão com o Azure Monitor no Grafana utilizando as informações no trecho abaixo, para conseguir essas informações será necessário entrar em contato comigo, 2º => Ajustar cada visão com a conexão que você criou.  


```
{
    "tenant_id": "XXXXXXXXXXXXXX",
    "client_id": "XXXXXXXXXXXXXXXXXX",
    "client_secret": "XXXXXXXXXXXXXXXXXX",
    "subscription_id": "XXXXXXXXXXXXXXXXXX"
}
```

# 🚀 Próximo Passo: Acesso ao Azure Cloud

A infraestrutura **on-premises** (local) do Case Compass foi configurada com sucesso.

Agora, o foco é a transição para o ambiente Cloud: a **estrutura de Dados no Azure** (incluindo Data Lake, Databricks e recursos de processamento) já está provisionada e pronta para uso.

> [!IMPORTANT]
>
> O acesso ao ambiente Azure Cloud será feito através de uma **conta de terceiro**.
>
> **Ação Requerida:** Entre em contato diretamente para solicitar e receber o **usuário e senha** de acesso à Cloud.

---

# 8. Melhorias do Case e Considerações Finais


O case desenvolvido tem como foco principal evidenciar o valor estratégico da Engenharia de Dados na geração de insights significativos sobre a experiência do usuário, além de viabilizar ao time de negócios o acesso a dados reais tanto dos próprios clientes quanto dos concorrentes. A proposta busca não apenas promover uma visão aprofundada da jornada do cliente, mas também oferecer subsídios concretos para decisões orientadas por dados, fortalecendo a atuação da empresa em um mercado cada vez mais competitivo.


## 8.1 Melhorias do Case
---

A seguir, será listada os itens de sugestão de melhorias, evolução e contribuições - divididas em estrutura funcional e técnica:


**Funcional:**

- **Escalabilidade** – A arquitetura proposta foi pensada para ser escalável e adaptável a diferentes instituições do mesmo segmento. No case, utilizamos como base os aplicativos de algumas instituições concorrentes e da própria INSTITUIÇÃO, mas como parte da evolução funcional, fica como sugestão a inclusão de novos pipelines (DAGs no Airflow) para ingestão e tratamento de dados de aplicativos de outros concorrentes. Isso possibilita comparações mais amplas e estratégicas entre os players do mercado.
- **Enriquecimento com Dados Externos** –  Incorporar fontes de dados externas adicionais, como Google Play, Reclame Aqui ou redes sociais, pode oferecer uma visão ainda mais ampla e contextualizada sobre a percepção do cliente. Esse enriquecimento auxilia na construção de análises mais precisas e na priorização de problemas críticos para o negócio.
- **Segmento por área** – Construção  do dashboard funcional (Power BI) com a inclusão de visões que ajudam o time de negócios a evoluir os próprios produtos, como PIX, Cartões, Contas, Consórcios, entre outros. Essa segmentação permite análises mais direcionadas, facilita a priorização de ações por equipe e contribui para uma visualização estratégica dos indicadores conforme a estrutura organizacional da instituição.



**Técnicas:**

- **Camada de Observabilidade** – Inserção de alertas automáticos no Grafana vinculados à falha de execução de jobs. Esses alertas serão classificados conforme a criticidade, considerando o impacto direto no pipeline e na entrega final dos dados ao cliente.
- **Camada de Observabilidade** – Ampliação da visão atual do dashboard de sustentação, que hoje é focado em métricas de aplicações Spark, para também contemplar o status das DAGs no Airflow. Essa melhoria visa cobrir cenários onde o job Spark não chega a ser executado por falhas no ambiente, variáveis de entrada incorretas, ou outros problemas de orquestração que atualmente não são capturados. Isso garante uma visão mais completa da saúde da aplicação e contribui para uma resposta mais rápida a falhas.
- **Camada de Observabilidade** – Implementar alertas automáticos no Grafana vinculados à camada de validação dos dados no pipeline. Essa validação ao encontrar uma irregularidade, gere um alerta para o time de responsável por sustentação a aplicação em sustentação, onde é verificado regras de integridade, conformidade de schema e verificação de valores nulos. Com isso, é possível detectar inconsistências em tempo real, reduzir riscos operacionais e assegurar a confiabilidade dos dados utilizados nas análises e decisões estratégicas.

## 8.2 Melhorias e Considerações Finais


O Case Compass reforça o papel da Engenharia de Dados como elemento central na construção de soluções voltadas para o negócio e para a experiência do usuário. Ao oferecer uma estrutura confiável, escalável e orientada à geração de *insights*, a iniciativa empodera times de produto com dados relevantes sobre seus próprios aplicativos e fornece uma base comparativa frente aos concorrentes.

Com isso, o Compass se torna uma ferramenta valiosa para instituições que buscam não só entender, mas também **antecipar as necessidades dos seus clientes** — fortalecendo sua presença no mercado e avançando na jornada rumo à principalidade financeira.


# 9. Referências

## MongoDB
- [MongoDB Pricing](https://www.mongodb.com/pricing) — Página oficial de preços do MongoDB Atlas.
- [MongoDB Atlas Pricing Calculator](https://www.mongodb.com/pricing/calculator/estimate/68e98b23fa34fd8b979250d0/cluster-configuration/68e98b23f31df20c03263b9c?isPrimaryConfigExpanded=true&isAdvancedConfigExpanded=false&isBackupNetworkExpanded=false&name=Cluster+1&instanceSize=M30&diskSizeGb=32&shardCount=1) — Estimativa detalhada de custos baseada em clusters M10, M20 e M30.

## Azure Databricks
- [Microsoft Learn — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/) — Documentação oficial sobre o uso e configuração do Azure Databricks.
- [Databricks Glossary — Data Lakehouse](https://www.databricks.com/br/glossary/data-lakehouse) — Conceitos e arquitetura da abordagem Lakehouse.

## Orquestração e Processamento de Dados
- [Apache Airflow Documentation](https://airflow.apache.org/docs/) — Guia oficial e recursos sobre orquestração de pipelines de dados.
- [Apache Spark Documentation (v3.5.0)](https://spark.apache.org/docs/3.5.0/) — Documentação da versão utilizada para o processamento distribuído no projeto Compass.
