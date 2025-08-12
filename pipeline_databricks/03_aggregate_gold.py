# Databricks notebook source
# Databricks Notebook: 03_aggregate_gold.ipynb

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, sum, count, when, countDistinct

print("Iniciando a agregação de dados da SILVER para a GOLD (KPIs)...")

# --- Carregando dados da Camada SILVER ---
silver_table_name = "legal_analytics_db.silver_processos_juridicos"
silver_table_path = "/mnt/legal_analytics/silver/processos_limpos" # Caminho para a tabela Silver

# Certifique-se de que a base de dados está em uso
spark.sql("USE legal_analytics_db;")

try:
    df_silver = spark.table(silver_table_name)
    print(f"Dados lidos da tabela Delta SILVER: {silver_table_name}")
except Exception as e:
    print(f"Não foi possível ler a tabela {silver_table_name} diretamente, tentando pelo path. Erro: {e}")
    df_silver = spark.read.format("delta").load(silver_table_path)
    print(f"Dados lidos do path Delta SILVER: {silver_table_path}")

#print("Schema da Camada Silver:")
#df_silver.printSchema()
#print("\nExemplo de Dados na Camada Silver (primeiras 5 linhas):")
#df_silver.show(5, truncate=False)

# --- Camada GOLD: Agregações para KPIs ---

# KPI 1: Taxa de Sucesso por Área Jurídica
kpi_sucesso_area = df_silver.filter(col("status") == "Finalizado") \
    .groupBy("area_juridica") \
    .agg(
        round(sum(when(col("resultado_final") == "Ganho", 1).otherwise(0)) / count("*") * 100, 2).alias("taxa_sucesso_ganho_pct"),
        round(sum(when(col("resultado_final") == "Acordo", 1).otherwise(0)) / count("*") * 100, 2).alias("taxa_acordo_pct"),
        count("*").alias("total_processos_finalizados")
    ) \
    .orderBy("taxa_sucesso_ganho_pct", ascending=False)

gold_kpi1_path = "/mnt/legal_analytics/gold/kpi_sucesso_area"
gold_kpi1_name = "gold_kpi_sucesso_area"
kpi_sucesso_area.write.format("delta").mode("overwrite").save(gold_kpi1_path)
spark.sql(f"CREATE TABLE IF NOT EXISTS {gold_kpi1_name} USING DELTA LOCATION '{gold_kpi1_path}';")
spark.sql(f"ALTER TABLE {gold_kpi1_name} SET LOCATION '{gold_kpi1_path}';")
df1 = spark.sql(f"SELECT * FROM {gold_kpi1_name}")

print("\nKPI: Taxa de Sucesso e Acordo por Área Jurídica (Processos Finalizados)")
display(df1)
#kpi_sucesso_area.show(truncate=False)

# KPI 2: Tempo Médio de Resolução por Tipo de Processo e Instância
kpi_tempo_resolucao = df_silver.filter(col("status") == "Finalizado") \
    .groupBy("tipo_processo", "instancia") \
    .agg(
        round(sum("duracao_dias") / count("*"), 2).alias("tempo_medio_dias"),
        count("*").alias("total_processos_finalizados")
    ) \
    .orderBy("tempo_medio_dias", ascending=False)

gold_kpi2_path = "/mnt/legal_analytics/gold/kpi_tempo_resolucao"
gold_kpi2_name = "gold_kpi_tempo_resolucao"
kpi_tempo_resolucao.write.format("delta").mode("overwrite").save(gold_kpi2_path)
spark.sql(f"CREATE TABLE IF NOT EXISTS {gold_kpi2_name} USING DELTA LOCATION '{gold_kpi2_path}';")
spark.sql(f"ALTER TABLE {gold_kpi2_name} SET LOCATION '{gold_kpi2_path}';")
df2 = spark.sql(f"SELECT * FROM {gold_kpi2_name}")

print("\nKPI: Tempo Médio de Resolução (dias) por Tipo de Processo e Instância")
display(df2)
#kpi_tempo_resolucao.show(truncate=False)

# KPI 3: Custo Médio por Processo e Resultado Final
kpi_custo_resultado = df_silver.filter(col("status") == "Finalizado") \
    .groupBy("resultado_final") \
    .agg(
        round(sum("custo_total_processo") / count("*"), 2).alias("custo_medio_total"),
        round(sum("custos_legais") / count("*"), 2).alias("custo_medio_custas"),
        round(sum("honorarios_advogado") / count("*"), 2).alias("custo_medio_honorarios"),
        round(sum("valor_causa") / count("*"), 2).alias("valor_causa_medio"),
        count("*").alias("total_processos_finalizados")
    ) \
    .orderBy("custo_medio_total", ascending=False)

gold_kpi3_path = "/mnt/legal_analytics/gold/kpi_custo_resultado"
gold_kpi3_name = "gold_kpi_custo_resultado"
kpi_custo_resultado.write.format("delta").mode("overwrite").save(gold_kpi3_path)
spark.sql(f"CREATE TABLE IF NOT EXISTS {gold_kpi3_name} USING DELTA LOCATION '{gold_kpi3_path}';")
spark.sql(f"ALTER TABLE {gold_kpi3_name} SET LOCATION '{gold_kpi3_path}';")
df3 = spark.sql(f"SELECT * FROM {gold_kpi3_name}")

print("\nKPI: Custo Médio por Processo por Resultado Final")
display(df3)
#kpi_custo_resultado.show(truncate=False)

# KPI 4: Distribuição de Processos por Responsável Interno e Escritório Externo (não é uma agregação final em si, mas um count)
kpi_distribuicao_responsavel = df_silver.groupBy("responsavel_interno", "escritorio_externo") \
    .agg(count("*").alias("total_processos")) \
    .orderBy("total_processos", ascending=False)

gold_kpi4_path = "/mnt/legal_analytics/gold/kpi_distribuicao_responsavel"
gold_kpi4_name = "gold_kpi_distribuicao_responsavel"
kpi_distribuicao_responsavel.write.format("delta").mode("overwrite").save(gold_kpi4_path)
spark.sql(f"CREATE TABLE IF NOT EXISTS {gold_kpi4_name} USING DELTA LOCATION '{gold_kpi4_path}';")
spark.sql(f"ALTER TABLE {gold_kpi4_name} SET LOCATION '{gold_kpi4_path}';")
df4 = spark.sql(f"SELECT * FROM {gold_kpi4_name}")

print("\nKPI: Distribuição de Processos por Responsável Interno e Escritório Externo")
display(df4)
#kpi_distribuicao_responsavel.show(truncate=False)

print("\nAgregações da camada GOLD concluídas. Tabelas criadas no metastore do Databricks.")