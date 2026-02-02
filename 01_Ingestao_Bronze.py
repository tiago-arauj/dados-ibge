# Databricks notebook source
# DBTITLE 1,Configuração de conta
# MAGIC %skip
# MAGIC spark.conf.set(
# MAGIC     "fs.azure.account.key.dadosibge.dfs.core.windows.net",
# MAGIC     "Yi/lmzNtzM2bjTXvKhCr1CaQHFhgUHQw9d8t18eOwHW2YlSxLt8auhGPC2ntEfXF4ez3vP+VFi04+ASt7i3hbw=="
# MAGIC )

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.dadosibge.dfs.core.windows.net",
    "Yi/lmzNtzM2bjTXvKhCr1CaQHFhgUHQw9d8t18eOwHW2YlSxLt8auhGPC2ntEfXF4ez3vP+VFi04+ASt7i3hbw=="
)

# 1. Definição dos caminhos dos notebooks
notebooks = [
    {"path": "./00_Config", "name": "Configuração de Conta"},
    {"path": "./01_Ingestao_Bronze", "name": "Camada Bronze (Extração IBGE)"},
    {"path": "./02_Transformacao_Silver", "name": "Camada Silver (Tratamento)"},
    {"path": "./03_Agregacao_Gold", "name": "Camada Gold (Agregação)"}
]

# 2. Execução sequencial com tratamento de erro
for nb in notebooks:
    try:
        print(f"Iniciando: {nb['name']}...")
        
        # O timeout de 3600 segundos (1 hora) é opcional
        status = dbutils.notebook.run(nb['path'], timeout_seconds=300)
        
        print(f"Sucesso: {nb['name']} finalizado com status: {status}")
        
    except Exception as e:
        print(f"Erro no notebook {nb['name']}:")
        print(e)
        
        # Interrompe o pipeline se uma camada falhar para evitar inconsistência
        dbutils.notebook.exit(f"Pipeline interrompido devido a erro no notebook: {nb['name']}")
        break

# COMMAND ----------

# DBTITLE 1,Camada Bronze
spark.conf.set(
    "fs.azure.account.key.dadosibge.dfs.core.windows.net",
    "Yi/lmzNtzM2bjTXvKhCr1CaQHFhgUHQw9d8t18eOwHW2YlSxLt8auhGPC2ntEfXF4ez3vP+VFi04+ASt7i3hbw=="
)

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