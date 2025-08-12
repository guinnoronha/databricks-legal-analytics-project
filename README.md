# Projeto Legal Analytics

Este projeto demonstra como aplicar Ciência de Dados e Engenharia de Dados (utilizando o modelo Medallion) para analisar indicadores de processos jurídicos de uma grande empresa. O objetivo é fornecer insights valiosos para o departamento jurídico, otimizando a tomada de decisões, a alocação de recursos e a gestão de riscos.

## Sumário

- [DataViz](#dataviz)
- [Visão Geral](#visão-geral)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Arquitetura do Projeto](#arquitetura-do-projeto)
- [Modelo Medallion](#modelo-medallion)
- [Base de Dados](#base-de-dados)
- [Análises e Insights Chave](#análises-e-insights-chave)
- [Tecnologias Utilizadas](#tecnologias-utilizadas)
- [Contato](#contato)

---

## DataViz

<img width="2056" height="1154" alt="image" src="https://github.com/user-attachments/assets/c080831d-f19f-40f9-bdbb-f82c48ceaa0d" />


**Link:** https://app.powerbi.com/view?r=eyJrIjoiYTg5YTZkMmMtNWUxYy00ODMyLWFlNmMtODVlZWE0NzQzMzM1IiwidCI6ImI5MjAzNTQwLWJmYjEtNDdhMi05ZTJiLWQ3M2VjMGRlYmY0OSJ9


---

## Visão Geral

O projeto "Legal Process Analytics" foca na transformação de dados brutos de processos jurídicos em informações acionáveis. Através da criação de um pipeline de dados no **Databricks Community Edition**, utilizando o modelo **Medallion Architecture**, limpamos, padronizamos e agregamos dados para gerar KPIs (Key Performance Indicators) essenciais para a gestão jurídica.

A arquitetura modular, dividida em notebooks por camada (Bronze, Silver, Gold) e análises, facilita a manutenção, o desenvolvimento e a orquestração do pipeline de dados.

### Problemas que o Projeto Ajuda a Resolver:
- Longos tempos de resolução de processos.
- Custos jurídicos elevados e pouco transparentes.
- Dificuldade em avaliar a performance de equipes internas e escritórios externos.
- Falta de insights sobre o impacto do risco inicial dos processos.

---

## Estrutura do Projeto

O projeto é organizado em quatro notebooks principais, seguindo o padrão Medallion Architecture:

.<br>
├── notebooks/<br>
│   ├── 01_generate_data_bronze.ipynb   # Geração de dados fictícios e carga para a camada Bronze<br>
│   ├── 02_transform_silver.ipynb       # Limpeza e transformação da Bronze para a Silver<br>
│   ├── 03_aggregate_gold.ipynb         # Agregações e KPIs da Silver para a Gold<br>
│   └── 04_analysis_insights.ipynb      # Análises e visualizações utilizando Silver e Gold<br>
└── README.md     

---

## Arquitetura do Projeto

O projeto foi idealizado através do Databricks para ingestão e tratamento dos dados:

![Logotipo da empresa](https://hermes.dio.me/articles/cover/6aa670f9-9bd3-4690-b529-44c35f6758a3.png)

---

## Modelo Medallion

Este projeto implementa o modelo Medallion para garantir a qualidade e a governança dos dados em cada etapa do pipeline:

-   **Bronze (Raw):**
    -   **Notebook:** `01_generate_data_bronze.ipynb`
    -   **Conteúdo:** Contém os dados brutos e inalterados, gerados sinteticamente e carregados como uma tabela Delta no Databricks. É o estágio de "aterragem" dos dados.
    
-   **Silver (Cleaned/Staging):**
    -   **Notebook:** `02_transform_silver.ipynb`
    -   **Conteúdo:** Dados limpos, com tipos de dados corrigidos, valores nulos tratados, colunas calculadas (ex: duração do processo) e padronizados. Nesta camada, os dados estão prontos para análises mais detalhadas e construções de modelos.

-   **Gold (Aggregated/Curated):**
    -   **Notebook:** `03_aggregate_gold.ipynb`
    -   **Conteúdo:** Contém os KPIs e agregações sumarizadas, otimizadas para consumo por dashboards, relatórios de gestão e aplicações de BI.

---

## Base de Dados

O projeto utiliza um conjunto de dados fictícios, gerado diretamente no Databricks. Este conjunto simula um volume de processos jurídicos de uma grande, incluindo informações como:

-   `id_processo`: Identificador único do processo.
-   `tipo_processo`: Cível, Trabalhista, Tributário, etc.
-   `area_juridica`: Contratos, RH, Fiscal, Imobiliário, etc.
-   `data_distribuicao`: Data de início do processo.
-   `data_sentenca`: Data da sentença (se houver).
-   `data_transito_julgado`: Data do trânsito em julgado (se houver).
-   `status`: Ativo, Finalizado.
-   `valor_causa`: Valor monetário da causa.
-   `resultado_final`: Ganho, Perdido, Acordo, Em Andamento.
-   `custos_legais`: Custas e despesas judiciais.
-   `honorarios_advogado`: Honorários pagos ao advogado.
-   `responsavel_interno`: Advogado(a) interno(a) responsável.
-   `escritorio_externo`: Escritório de advocacia externo.
-   `tribunal`: Tribunal onde o processo tramita.
-   `comarca`: Comarca/cidade do processo.
-   `instancia`: Primeira Instância, Segunda Instância, Administrativo.
-   `risco_inicial`: Baixo, Médio, Alto, Muito Alto.

---

## Análises e Insights Chave

O notebook `04_analysis_insights.ipynb` utiliza os dados processados e agregados para gerar diversos KPIs e insights, incluindo:

1.  **Taxa de Sucesso por Área Jurídica:** Percentual de processos ganhos e acordados por cada área, ajudando a identificar áreas de força e fraqueza.
2.  **Tempo Médio de Resolução:** Duração média dos processos por tipo e instância, revelando gargalos e ineficiências.
3.  **Custo Médio por Resultado Final:** Análise dos custos (totais, custas, honorários) associados a cada desfecho do processo (ganho, perdido, acordo), informando decisões estratégicas.
4.  **Distribuição de Carga de Trabalho:** Visualização de quantos processos cada responsável interno e escritório externo gerencia.
5.  **Análise de Dispersão:** Gráfico comparando `Valor da Causa` vs. `Custo Total do Processo`, com insights sobre o impacto do resultado.
6.  **Comarcas Críticas:** Identificação das comarcas onde os processos tendem a ser mais longos.
7.  **Correlação Risco vs. Resultado:** Análise se o risco inicial do processo se alinha com o resultado final obtido.

Esses insights permitem ao departamento jurídico:
- Otimizar a alocação de recursos.
- Negociar prazos e honorários de forma mais embasada.
- Desenvolver estratégias proativas para mitigar riscos.
- Avaliar e melhorar a performance da equipe e parceiros.

---

## Tecnologias Utilizadas

-   **Databricks Community Edition:** Plataforma de Data & AI.
-   **Apache Spark (PySpark):** Para processamento distribuído de dados.
-   **Delta Lake:** Formato de armazenamento de dados otimizado para data lakes (transações ACID, *schema enforcement*).
-   **Pandas:** Para manipulação de dados em análises secundárias (dentro do Spark).
-   **Matplotlib e Seaborn:** Para visualização de dados.

---

## Contato

**Autor:** Guilherme Noronha Mello <br>
**Linkedin:** https://www.linkedin.com/in/guilherme-noronha-mello/ <br>
**GitHub:** https://github.com/guinnoronha
