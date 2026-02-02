# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.dadosibge.dfs.core.windows.net",
    "Yi/lmzNtzM2bjTXvKhCr1CaQHFhgUHQw9d8t18eOwHW2YlSxLt8auhGPC2ntEfXF4ez3vP+VFi04+ASt7i3hbw=="
)

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