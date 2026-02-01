# API IBGE (Tabela 6579)

Este projeto consiste em um pipeline de dados automatizado desenvolvido no **Azure Databricks**, utilizando **PySpark** e **Delta Lake**. O objetivo é extrair, tratar e organizar dados de estimativas populacionais da API SIDRA do IBGE, seguindo as melhores práticas de Engenharia de Dados.

##  Arquitetura da Solução

A solução foi estruturada utilizando a **Medallion Architecture** (Arquitetura Medalhão), garantindo a organização e a qualidade do dado em cada etapa do processo.



### 🥉 Camada Bronze (Raw)
* **Objetivo**: Ingestão do dado bruto direto da API.
* **Formato**: Delta Lake.
* **Decisão**: Armazenar o dado exatamente como ele vem da origem para garantir que possamos reprocessar o pipeline sem depender novamente da API externa.

### 🥈 Camada Silver (Tratada)
* **Objetivo**: Limpeza, padronização e tipagem.
* **Ações**: 
  * Remoção de cabeçalhos descritivos da API.
  * Renomeação de colunas técnicas (D1N, V, etc.) para nomes legíveis (UF, Populacao).
  * Conversão de tipos (ex: Populacao de `string` para `double`).
* **Qualidade**: Implementação de **Data Quality Gates** para validar nulidade e valores inconsistentes (negativos).

### 🥇 Camada Gold (Consumo)
* **Objetivo**: Tabela analítica pronta para negócios.
* **Agregação**: Criação de um ranking das 10 Unidades da Federação mais populosas por ano, utilizando Window Functions.

---

## Recursos utilizados 

* **Azure Databricks**: Ambiente de processamento distribuído.
* **Azure Data Lake Storage Gen2**: Armazenamento em nuvem escalável.
* **PySpark**: Engine de processamento de dados.
* **Delta Lake**: Formato de tabela que garante transações ACID e controle de versão (Time Travel).
* **API SIDRA (IBGE)**: Fonte de dados.

---

## Decisões Técnicas e Diferenciais

1. **Idempotência**: O pipeline foi construído com o modo `overwrite`, permitindo que o processo seja executado múltiplas vezes sem gerar duplicidade de dados.
2. **Governança de Dados**: A inclusão de uma célula de **Data Quality** garante que falhas na API ou dados corrompidos não cheguem à camada Gold, protegendo a análise do usuário final.
3. **Escalabilidade**: Ao utilizar PySpark e Delta Lake, a solução está preparada para lidar com grandes volumes de dados (Big Data), indo além da simples manipulação de arquivos locais.
4. **Resiliência**: O uso de blocos de repetição (`for`) nas camadas permite que o pipeline seja facilmente expandido para outras tabelas do IBGE com poucas alterações de código.

---

## Evidência da estrutura construida
1. **Estrutura de pastas**
2. ![portal-azure.png](jb-image:img_1769915205778_f76ff82132d65)
3. ![grupo-de-recurso.png](jb-image:img_1769915236300_efd27e687f7e9)
4. ![azure-databricks.png](jb-image:img_1769915259843_168791c36a21e)
5. ![containners.png](jb-image:img_1769915274815_83ccf4ca30b758)
1. **Bronze**: ![bronze_populacao.png](jb-image:img_1769914932113_cca9648a2d6eb8)
   ![bronze_populacao_raw.png](jb-image:img_1769914951048_7276df81bb935)
   ![bronze_populacao_raw_deltalog.png](jb-image:img_1769914967052_a680bbf8b74bf)
3. **Silver**: ![populacao-silver.png](jb-image:img_1769914998120_1eddf144581e68)
   ![populacao-silver_estrutura.png](jb-image:img_1769915017537_e461532808c3b8)
   ![populacao-silver_deltalog.png](jb-image:img_1769915028403_145ee6e3c753c)
5. **Gold**: ![gold_indicadores.png](jb-image:img_1769915070091_fecabea3e9e69)
   ![gold_ranking_populacao_uf.png](jb-image:img_1769915103514_570289290e2d08)
   ![gold_populacao_vs_media_regional.png](jb-image:img_1769915130274_bb9263b534de3)
   ![gold_representatividade_brasil.png](jb-image:img_1769915153090_c45ccb3e187da8)
   
---

## Como Executar

1. Importe o notebook `.ipynb` presente neste repositório para o seu Workspace do Databricks.
2. Configure as credenciais de acesso da sua **Azure Storage Account** na célula de configuração inicial.
3. Execute todas as células para processar as camadas Bronze, Silver e Gold sequencialmente.
