# Databricks notebook source
# Databricks Notebook: 02_transform_silver.ipynb

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, datediff, coalesce, lit, when
from pyspark.sql.types import DoubleType, DateType
from datetime import datetime

print("Iniciando a transformação de dados da BRONZE para a SILVER...")

# --- Carregando dados da Camada BRONZE ---
bronze_table_name = "legal_analytics_db.bronze_processos_juridicos"
bronze_table_path = "/mnt/legal_analytics/bronze/processos_brutos" # Caminho para a tabela Bronze

# Certifique-se de que a base de dados está em uso
spark.sql("USE legal_analytics_db;")

# Tenta ler a tabela diretamente do metastore ou do caminho
try:
    df_bronze = spark.table(bronze_table_name)
    print(f"Dados lidos da tabela Delta BRONZE: {bronze_table_name}")
except Exception as e:
    print(f"Não foi possível ler a tabela {bronze_table_name} diretamente, tentando pelo path. Erro: {e}")
    df_bronze = spark.read.format("delta").load(bronze_table_path)
    print(f"Dados lidos do path Delta BRONZE: {bronze_table_path}")

print("Schema da Camada Bronze:")
df_bronze.printSchema()
print("\nExemplo de Dados na Camada Bronze (primeiras 5 linhas):")
df_bronze.show(5, truncate=False)

# --- Camada SILVER: Limpeza e Padronização ---
# Selecionar colunas e aplicar transformações de tipo e limpeza
df_silver = df_bronze.select(
    col("id_processo"),
    col("tipo_processo").cast("string"),
    col("area_juridica").cast("string"),
    to_date(col("data_distribuicao"), "yyyy-MM-dd").alias("data_distribuicao"),
    to_date(col("data_sentenca"), "yyyy-MM-dd").alias("data_sentenca"),
    to_date(col("data_transito_julgado"), "yyyy-MM-dd").alias("data_transito_julgado"),
    col("status").cast("string"),
    col("valor_causa").cast(DoubleType()),
    col("resultado_final").cast("string"),
    col("custos_legais").cast(DoubleType()),
    col("honorarios_advogado").cast(DoubleType()),
    col("responsavel_interno").cast("string"),
    col("escritorio_externo").cast("string"),
    col("tribunal").cast("string"),
    col("comarca").cast("string"),
    col("instancia").cast("string"),
    col("risco_inicial").cast("string")
)

# Tratamento de valores nulos/ausentes para cálculos futuros
# data_fim_processo: usa data_transito_julgado, senão data_sentenca, senão a data atual se processo ativo.
# Isso evita que processos ativos tenham duração negativa ou nula se data_distribuicao for o único campo.
df_silver = df_silver.withColumn(
    "data_fim_processo",
    when(col("status") == "Ativo", lit(datetime.now().strftime("%Y-%m-%d")).cast(DateType())) # Para processos ativos, use a data atual
    .otherwise(coalesce(col("data_transito_julgado"), col("data_sentenca"))) # Para finalizados, use transito ou sentença
)

# Calcular duração do processo em dias (data_fim_processo - data_distribuicao)
df_silver = df_silver.withColumn(
    "duracao_dias",
    datediff(col("data_fim_processo"), col("data_distribuicao"))
)

# Preencher nulos em colunas de valor com 0 para evitar erros em somas
df_silver = df_silver.na.fill(0.0, subset=["valor_causa", "custos_legais", "honorarios_advogado"])

# Definir a coluna 'custo_total_processo'
df_silver = df_silver.withColumn("custo_total_processo", col("custos_legais") + col("honorarios_advogado"))

# --- Salva os Dados Limpos na Camada SILVER ---
silver_table_path = "/mnt/legal_analytics/silver/processos_limpos"
silver_table_name = "silver_processos_juridicos"

df_silver.write.format("delta").mode("overwrite").save(silver_table_path)
spark.sql(f"CREATE TABLE IF NOT EXISTS {silver_table_name} USING DELTA LOCATION '{silver_table_path}';")
spark.sql(f"ALTER TABLE {silver_table_name} SET LOCATION '{silver_table_path}';")

print(f"\nDados limpos salvos com sucesso na tabela Delta SILVER: '{silver_table_name}' em '{silver_table_path}'")
print("\nSchema da Camada Silver:")
df_silver.printSchema()
print("\nExemplo de Dados na Camada Silver (primeiras 5 linhas):")
df_silver.show(5, truncate=False)