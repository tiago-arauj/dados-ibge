# Databricks notebook source
# DBTITLE 1,Camada Gold - Ranking populacao UF
spark.conf.set(
    "fs.azure.account.key.dadosibge.dfs.core.windows.net",
    "Yi/lmzNtzM2bjTXvKhCr1CaQHFhgUHQw9d8t18eOwHW2YlSxLt8auhGPC2ntEfXF4ez3vP+VFi04+ASt7i3hbw=="
)

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

print(f" Indicador de Média Regional salvo com sucesso em: {caminho_gold}")
display(df_gold_final.limit(20))