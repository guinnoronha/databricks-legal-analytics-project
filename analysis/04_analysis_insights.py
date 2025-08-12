# Databricks notebook source
# Databricks Notebook: 04_analysis_insights.ipynb

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, sum, count, when, countDistinct
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Configurações de visualização para Pandas e Matplotlib
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
plt.style.use('ggplot') # Um estilo de gráfico mais agradável

print("Iniciando análises e extração de insights das camadas SILVER e GOLD...")

# --- Carregando dados da Camada SILVER ---
silver_table_name = "legal_analytics_db.silver_processos_juridicos"
silver_table_path = "/mnt/legal_analytics/silver/processos_limpos" # Caminho para a tabela Silver

spark.sql("USE legal_analytics_db;") # Certifique-se de que a base de dados está em uso

try:
    df_silver = spark.table(silver_table_name)
    print(f"Dados lidos da tabela Delta SILVER para análises: {silver_table_name}")
except Exception as e:
    print(f"Não foi possível ler a tabela {silver_table_name} diretamente, tentando pelo path. Erro: {e}")
    df_silver = spark.read.format("delta").load(silver_table_path)
    print(f"Dados lidos do path Delta SILVER para análises: {silver_table_path}")

#print("\nExemplo de Dados da Camada SILVER:")
#df_silver.show(5, truncate=False)


# --- Carregando KPIs da Camada GOLD ---
kpi_sucesso_area = spark.table("legal_analytics_db.gold_kpi_sucesso_area")
kpi_tempo_resolucao = spark.table("legal_analytics_db.gold_kpi_tempo_resolucao")
kpi_custo_resultado = spark.table("legal_analytics_db.gold_kpi_custo_resultado")
kpi_distribuicao_responsavel = spark.table("legal_analytics_db.gold_kpi_distribuicao_responsavel")

#print("\nKPIs carregados da camada GOLD:")
#print("\nKPI: Taxa de Sucesso por Área Jurídica:")
#kpi_sucesso_area.show(truncate=False)
#print("\nKPI: Tempo Médio de Resolução por Tipo de Processo e Instância:")
#kpi_tempo_resolucao.show(truncate=False)
#print("\nKPI: Custo Médio por Processo por Resultado Final:")
#kpi_custo_resultado.show(truncate=False)
#print("\nKPI: Distribuição de Processos por Responsável Interno e Escritório Externo:")
#kpi_distribuicao_responsavel.show(truncate=False)


# --- Análises e Insights Aprofundados ---

print("\n--- Análises e Insights Aprofundados ---")

# Análise 1: Distribuição de Processos por Risco Inicial (usando Silver)
print("\nDistribuição de Processos por Risco Inicial:")
df_silver_risco_count = df_silver.groupBy("risco_inicial").count().orderBy("count", ascending=False).toPandas()
plt.figure(figsize=(8, 5))
ax = sns.barplot(x='risco_inicial', y='count', data=df_silver_risco_count, palette='viridis')
sns.barplot(x='risco_inicial', y='count', data=df_silver_risco_count, palette='viridis')
plt.title('Distribuição de Processos por Risco Inicial')
plt.xlabel('Nível de Risco')
plt.ylabel('Número de Processos')
plt.grid(axis='y', linestyle='--', alpha=0.7)
for container in ax.containers:
    ax.bar_label(container, fmt='%d')
plt.show()

# Insight: A maioria dos processos é classificada como 'Médio', mas a empresa deve ficar atenta aos 'Alto' e 'Muito Alto'.

# Análise 2: Custo Total por Escritório Externo (usando Silver)
print("\nCusto Total por Escritório Externo:")
df_silver_custo_escritorio = df_silver.groupBy("escritorio_externo") \
    .agg(round(sum("custo_total_processo"), 2).alias("custo_total_gastos")) \
    .orderBy("custo_total_gastos", ascending=False).toPandas()

plt.figure(figsize=(12, 7))
sns.barplot(x='escritorio_externo', y='custo_total_gastos', data=df_silver_custo_escritorio, palette='cividis')
plt.title('Custo Total de Processos por Escritório Externo')
plt.xlabel('Escritório Externo')
plt.ylabel('Custo Total (R$)')
plt.xticks(rotation=45, ha='right')
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()
plt.show()

# Insight: Identifica os escritórios mais caros. Isso pode levar a negociações de honorários ou realocação de casos.

# Análise 3: Valor da Causa vs. Custo Total do Processo por Resultado (usando Silver)
print("\nAnálise de Dispersão: Valor da Causa vs. Custo Total (Processos Finalizados)")
df_plot_finalizados = df_silver.filter(col("status") == "Finalizado").toPandas()

plt.figure(figsize=(12, 8))
sns.scatterplot(x='valor_causa', y='custo_total_processo', hue='resultado_final', 
                size='valor_causa', sizes=(20, 400), # Tamanho do ponto baseado no valor da causa
                alpha=0.7, data=df_plot_finalizados, palette='viridis')
plt.title('Valor da Causa vs. Custo Total do Processo por Resultado Final')
plt.xlabel('Valor da Causa (R$)')
plt.ylabel('Custo Total do Processo (R$)')
plt.xscale('log') # Escala logarítmica para melhor visualização de valores de causa amplos
plt.yscale('log') # Escala logarítmica para melhor visualização de custos amplos
plt.grid(True, which="both", ls="--", c="0.7")
plt.legend(title='Resultado Final')
plt.tight_layout()
plt.show()

# Insight: Observa-se que mesmo processos de baixo valor de causa podem gerar custos totais consideráveis,
# especialmente se resultarem em "Perdido". Processos "Ganho" tendem a ter custos mais previsíveis.

# Análise 4: Tempo de Processo por Comarca (Top 5 mais lentas) (usando Silver)
print("\nTop 5 Comarcas com Maior Tempo Médio de Resolução (excluindo aquelas com poucos processos):")
df_silver_comarca_tempo = df_silver.filter(col("status") == "Finalizado") \
    .groupBy("comarca") \
    .agg(round(sum("duracao_dias") / count("*"), 2).alias("tempo_medio_dias"),
         count("*").alias("total_processos")) \
    .filter(col("total_processos") >= 5) \
    .orderBy("tempo_medio_dias", ascending=False) \
    .limit(5).toPandas()

plt.figure(figsize=(10, 6))
sns.barplot(x='comarca', y='tempo_medio_dias', data=df_silver_comarca_tempo, palette='plasma')
plt.title('Top 5 Comarcas com Maior Tempo Médio de Resolução')
plt.xlabel('Comarca')
plt.ylabel('Tempo Médio (dias)')
plt.xticks(rotation=45, ha='right')
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()
plt.show()

# Insight: Destaca as comarcas onde os processos tendem a ser mais demorados, o que pode influenciar a estratégia de litígios ou a alocação de equipes.

# Análise 5: Correlação entre Risco Inicial e Resultado Final (usando Silver)
print("\nCorrelação entre Risco Inicial e Resultado Final (Processos Finalizados):")
df_silver_risco_resultado = df_silver.filter(col("status") == "Finalizado") \
    .groupBy("risco_inicial", "resultado_final").count().toPandas()

# Pivotar para melhor visualização
pivot_table = df_silver_risco_resultado.pivot_table(index='risco_inicial', columns='resultado_final', values='count', fill_value=0)
print(pivot_table)

# Visualização em um mapa de calor
plt.figure(figsize=(8, 6))
sns.heatmap(pivot_table, annot=True, fmt="d", cmap="YlGnBu", linewidths=.5)
plt.title('Matriz de Risco Inicial vs. Resultado Final')
plt.xlabel('Resultado Final')
plt.ylabel('Risco Inicial')
plt.tight_layout()
plt.show()

# Insight: Permite verificar se processos com "Alto" ou "Muito Alto" risco inicial realmente resultam mais frequentemente em "Perdido" ou "Acordo", validando o sistema de classificação de risco.

print("\nAnálises e insights concluídos. Explore os gráficos gerados para mais detalhes.")