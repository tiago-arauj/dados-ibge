# Databricks notebook source

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
        
        status = dbutils.notebook.run(nb['path'], timeout_seconds=300)
        
        print(f"Sucesso: {nb['name']} finalizado com status: {status}")
        
    except Exception as e:
        print(f"Erro no notebook {nb['name']}:")
        print(e)
        
        # Interrompe o pipeline se uma camada falhar para evitar inconsistência
        dbutils.notebook.exit(f"Pipeline interrompido devido a erro no notebook: {nb['name']}")
        break