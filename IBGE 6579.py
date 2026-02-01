# Databricks notebook source
import os 
from dotenv import load_dotenv

load_dotenv()

storage_key = os.getenv("STORAGE_KEY")
storage_account = os.getenv("STORAGE_ACCOUNT")


# COMMAND ----------

# DBTITLE 1,Configuração de conta
spark.conf.set(
    "fs.azure.account.key.dadosibge.dfs.core.windows.net",
    "Yi/lmzNtzM2bjTXvKhCr1CaQHFhgUHQw9d8t18eOwHW2YlSxLt8auhGPC2ntEfXF4ez3vP+VFi04+ASt7i3hbw=="
)

# COMMAND ----------

# DBTITLE 1,Camada Bronze
import requests
import json
from pyspark.sql import Row

# 1. Extração da Tabela 6579 (Estimativas de População)
url = "https://apisidra.ibge.gov.br/values/t/6579/n3/all/p/all/v/all"
response = requests.get(url)
data = response.json()

df_raw = spark.createDataFrame(data)

# 3. Salvar na Bronze
path_bronze = "abfss://bronze@dadosibge.dfs.core.windows.net/populacao_raw"
df_raw.write.format("delta").mode("overwrite").save(path_bronze)

display(df_raw.limit(5))

# COMMAND ----------

# DBTITLE 1,Camada silver
from pyspark.sql.functions import col

# 1. Definições de Caminho 
STORAGE_ACCOUNT = "dadosibge"  
container_bronze = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/populacao_raw"
tabelas_processamento = ["populacao_silver"]

for tabela in tabelas_processamento:
    # 2. Leitura dos dados brutos da Bronze
    df_silver_raw = spark.read.format("delta").load(container_bronze)

    # 3. Transformação: Filtro de cabeçalho, renomeação e tipagem
    df_silver_clean = df_silver_raw.filter(col("V") != "Valor").select(
         col("D1C").alias("Cod_UF")
        ,col("D1N").alias("UF")
        ,col("D2N").alias("Ano")
        ,col("D3C").alias("Cod_Variavel")
        ,col("D3N").alias("Variavel")
        ,col("MC").alias("Cod_Unidade_Medida")
        ,col("MN").alias("Unidade_Medida")
        ,col("NC").alias("Cod_Nivel_Territorial")
        ,col("NN").alias("Nivel_Territorial")
        ,col("V").cast("double").alias("Populacao")
    )

    caminho_destino = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/{tabela}"
    
    df_silver_clean.write.format("delta") \
        .mode("overwrite") \
        .save(caminho_destino)

    print(f"Camada silver '{tabela}' salva com sucesso em: {caminho_destino}")
    display(df_silver_clean.limit(5))

# COMMAND ----------

# DBTITLE 1,Data Quality
from pyspark.sql.functions import col
STORAGE_ACCOUNT = "dadosibge"
container_silver = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/populacao_silver"

df_check = spark.read.format("delta").load(container_silver)

null_count = df_check.filter(col("UF").isNull() | col("Ano").isNull()).count()

negative_populacao = df_check.filter(col("Populacao") < 0).count()

if null_count ==0 and negative_populacao ==0:
    print(f"Data quality: Passou, dados integros")
else:
    print(f" Data quality: Falhou. Encontrado {null_count} nulos e {negative_populacao} negativos")
    raise Exception("Dados inconsistentes na camada silver. Parando a execução do pipeline")

# COMMAND ----------

# DBTITLE 1,Camada Gold - Ranking populacao UF
from pyspark.sql.functions import col, desc, rank
from pyspark.sql.window import Window

STORAGE_ACCOUNT = "dadosibge"
container_silver = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/populacao_silver"
tabela_gold = "ranking_populacao_uf"

df_gold_input = spark.read.format("delta").load(container_silver)

# 3. Transformação: Criando um Ranking das 10 maiores populações por Ano
window_spec = Window.partitionBy("Ano").orderBy(desc("Populacao"))

df_gold_final = df_gold_input.withColumn("Ranking", rank().over(window_spec)) \
    .filter(col("Ranking") <= 10) \
    .select("Ano", "Ranking", "UF", "Populacao", "Unidade_Medida")

# 4. Salvando na Camada Gold
caminho_gold = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net/{tabela_gold}"

df_gold_final.write.format("delta").mode("overwrite").save(caminho_gold)

print(f"Camada Gold '{tabela_gold}' salva com sucesso em: {caminho_gold}")
display(df_gold_final)

# COMMAND ----------

# DBTITLE 1,Camada Gold (Representatividade Nacional (% do Total))
from pyspark.sql.window import Window
from pyspark.sql.functions import col, desc, round, sum

STORAGE_ACCOUNT = "dadosibge"
container_silver = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/populacao_silver"
tabela_gold = "representatividade_nacional_brasil"

df_silver = spark.read.format("delta").load(container_silver)

df_silver = df_silver.withColumn("Populacao", col("Populacao").cast("double"))

# 3. Transformação: Criando a métrica de Share Nacional por Ano
window_brasil = Window.partitionBy("Ano")

df_share = df_silver.withColumn("Total_Brasil", sum("Populacao").over(window_brasil)) \
    .withColumn("Percentual_nacional", round((col("Populacao") / col("Total_Brasil")) * 100, 2))

df_share_ordenado = df_share.orderBy(desc("Percentual_nacional"))

# 4. Salvando na Camada Gold
caminho_gold = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net/{tabela_gold}"

df_share.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(caminho_gold)

print(f"Camada Gold '{tabela_gold}' salva com sucesso em: {caminho_gold}")
display(df_share.limit(10))

# COMMAND ----------

# DBTITLE 1,(Gold) Diferença em relação à Média Regional
from pyspark.sql.functions import col, avg, round, substring
from pyspark.sql.window import Window

STORAGE_ACCOUNT = "dadosibge"
container_silver = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/populacao_silver"
tabela_gold = "populacao_vs_media_regional"

df_silver = spark.read.format("delta").load(container_silver)

# 3. Transformação e Criação de Indicadores
df_analise = df_silver.withColumn("Populacao", col("Populacao").cast("double")) \
    .withColumn("Cod_Regiao", substring(col("Cod_UF"), 1, 1))

# definido a janela particionada por Ano e Região
window_regional = Window.partitionBy("Ano", "Cod_Regiao")

df_gold_regional = df_analise.withColumn("Media_Regional_Ano", avg("Populacao").over(window_regional)) \
    .withColumn("Media_Regional_Ano", round(col("Media_Regional_Ano"), 2)) \
    .withColumn("Diferenca_da_Media", round(col("Populacao") - col("Media_Regional_Ano"), 2)) \
    .select(
        "Ano", 
        "Cod_Regiao", 
        "UF", 
        "Populacao", 
        "Media_Regional_Ano", 
        "Diferenca_da_Media"
    )

df_gold_final = df_gold_regional.orderBy(col("Ano").desc(), col("Diferenca_da_Media").desc())

# 5. Salvando na Camada Gold
caminho_gold = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net/{tabela_gold}"

df_gold_final.write.format("delta") \
    .mode("overwrite") \
    .save(caminho_gold)

print(f"✅ Indicador de Média Regional salvo com sucesso em: {caminho_gold}")
display(df_gold_final.limit(20))