# Databricks notebook source
# Databricks Notebook: 01_generate_data_bronze.ipynb

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from pyspark.sql.functions import to_date, col # Embora não usado diretamente para a geração, é bom para contexto

print("Iniciando a geração de dados fictícios e carregamento para a camada BRONZE...")

def generate_legal_data(num_rows=1000):
    """
    Gera um DataFrame Pandas com dados fictícios de processos jurídicos.

    Args:
        num_rows (int): Número de linhas de dados a serem geradas.

    Returns:
        pd.DataFrame: DataFrame Pandas contendo os dados dos processos.
    """

    data = []

    # Listas de valores possíveis
    tipos_processo = ['Cível', 'Trabalhista', 'Tributário', 'Regulatório', 'Ambiental', 'Societário', 'Propriedade Intelectual']
    areas_juridicas = {
        'Cível': ['Contratos', 'Imobiliário', 'Consumidor', 'Recuperação de Crédito'],
        'Trabalhista': ['RH', 'Acidentes de Trabalho', 'Previdenciário'],
        'Tributário': ['Fiscal', 'Planejamento Tributário'],
        'Regulatório': ['Compliance', 'Setorial'],
        'Ambiental': ['Licenciamento', 'Sanções'],
        'Societário': ['Fusões e Aquisições', 'Governança'],
        'Propriedade Intelectual': ['Patentes', 'Marcas', 'Direitos Autorais']
    }
    status_processo = ['Ativo', 'Finalizado']
    resultados_finais = ['Ganho', 'Perdido', 'Acordo']
    responsaveis_internos = ['Ana Silva', 'Bruno Costa', 'Carla Souza', 'Daniel Oliveira', 'Fernanda Lima', 'Gustavo Pires']
    escritorios_externos = ['Advogados Associados LTDA', 'Jurídico Soluções', 'Lex Consultoria', 'Global Law', 'Martins & Oliveira Advogados', 'Santos & Almeida Consultoria']
    tribunais = ['TJSP', 'TJRJ', 'TJMG', 'TRT1', 'TRT2', 'TRT3', 'TRF1', 'TRF2', 'TRF3', 'STJ', 'STF', 'CARF', 'ANVISA', 'CADE']
    comarcas = ['São Paulo', 'Rio de Janeiro', 'Belo Horizonte', 'Campinas', 'Guarulhos', 'Osasco', 'Curitiba', 'Porto Alegre', 'Brasília', 'Salvador', 'Fortaleza', 'Recife', 'Vitória']
    instancias = ['Primeira Instância', 'Segunda Instância', 'Tribunais Superiores', 'Administrativo']
    riscos_iniciais = ['Baixo', 'Médio', 'Alto', 'Muito Alto']

    start_date = datetime(2020, 1, 1)

    for i in range(num_rows):
        id_processo = f"P_{i+1:04d}"
        tipo_processo = random.choice(tipos_processo)
        area_juridica = random.choice(areas_juridicas[tipo_processo]) if tipo_processo in areas_juridicas else "Outros"

        dist_days = random.randint(0, 1500)
        data_distribuicao = start_date + timedelta(days=dist_days)

        status = random.choices(status_processo, weights=[0.4, 0.6], k=1)[0]

        data_sentenca = None
        data_transito_julgado = None
        resultado_final = "Em Andamento"

        if status == 'Finalizado':
            sent_days = random.randint(30, 500)
            data_sentenca = data_distribuicao + timedelta(days=sent_days)

            tj_days = random.randint(15, 180)
            data_transito_julgado = data_sentenca + timedelta(days=tj_days)

            resultado_final = random.choices(resultados_finais, weights=[0.4, 0.3, 0.3], k=1)[0]

        valor_causa = round(random.uniform(10000.00, 10000000.00), 2)
        custos_legais = round(random.uniform(500.00, valor_causa * 0.05), 2)
        honorarios_advogado = round(random.uniform(1000.00, valor_causa * 0.1), 2)

        responsavel_interno = random.choice(responsaveis_internos)
        escritorio_externo = random.choice(escritorios_externos)
        tribunal = random.choice(tribunais)
        comarca = random.choice(comarcas)
        instancia = random.choice(instancias)
        risco_inicial = random.choice(riscos_iniciais)

        data.append([
            id_processo, tipo_processo, area_juridica,
            data_distribuicao.strftime('%Y-%m-%d'),
            data_sentenca.strftime('%Y-%m-%d') if data_sentenca else '',
            data_transito_julgado.strftime('%Y-%m-%d') if data_transito_julgado else '',
            status, valor_causa, resultado_final, custos_legais, honorarios_advogado,
            responsavel_interno, escritorio_externo, tribunal, comarca, instancia, risco_inicial
        ])

    columns = [
        'id_processo', 'tipo_processo', 'area_juridica', 'data_distribuicao',
        'data_sentenca', 'data_transito_julgado', 'status', 'valor_causa',
        'resultado_final', 'custos_legais', 'honorarios_advogado',
        'responsavel_interno', 'escritorio_externo', 'tribunal', 'comarca',
        'instancia', 'risco_inicial'
    ]

    df = pd.DataFrame(data, columns=columns)
    return df

# --- Geração do DataFrame Pandas ---
num_linhas = 1000 # Você pode alterar isso para testar volumes maiores!
df_pandas_generated = generate_legal_data(num_linhas)

# Convertendo o DataFrame Pandas para um DataFrame Spark
df_spark_generated = spark.createDataFrame(df_pandas_generated)

# --- Camada BRONZE: Salvando os Dados Brutos como Tabela Delta ---
# Define o caminho para a tabela Delta Bronze
bronze_table_path = "/mnt/legal_analytics/bronze/processos_brutos"
bronze_table_name = "bronze_processos_juridicos"

# Salva o DataFrame Spark como uma tabela Delta na camada Bronze
# 'overwrite' garante que cada execução reinicie os dados da bronze
df_spark_generated.write.format("delta").mode("overwrite").save(bronze_table_path)

# Cria ou substitui a tabela no metastore do Databricks
spark.sql(f"CREATE DATABASE IF NOT EXISTS legal_analytics_db;")
spark.sql(f"USE legal_analytics_db;")
spark.sql(f"CREATE TABLE IF NOT EXISTS {bronze_table_name} USING DELTA LOCATION '{bronze_table_path}';")
spark.sql(f"ALTER TABLE {bronze_table_name} SET LOCATION '{bronze_table_path}';") # Garante que a localização esteja correta, útil se a tabela já existir

print(f"Dados brutos salvos com sucesso na tabela Delta BRONZE: '{bronze_table_name}' em '{bronze_table_path}'")
print("\nExemplo de dados da camada BRONZE:")
spark.read.format("delta").load(bronze_table_path).show(5, truncate=False)