# API IBGE (Tabela 6579)

Este projeto consiste em um pipeline de dados automatizado desenvolvido no **Azure Databricks**, utilizando **PySpark** e **Delta Lake**. O objetivo é extrair, tratar e organizar dados de estimativas populacionais da API SIDRA do IBGE, seguindo as melhores práticas de Engenharia de Dados.

##  Arquitetura da Solução

A solução foi estruturada utilizando a **Medallion Architecture** (Arquitetura Medalhão), garantindo a organização e a qualidade do dado em cada etapa do processo.



###  Camada Bronze (Raw)
* **Objetivo**: Ingestão do dado bruto direto da API.
* **Formato**: Delta Lake.
* **Decisão**: Armazenar o dado exatamente como ele vem da origem para garantir que possamos reprocessar o pipeline sem depender novamente da API externa.

###  Camada Silver (Tratada)
* **Objetivo**: Limpeza, padronização e tipagem.
* **Ações**: 
  * Remoção de cabeçalhos descritivos da API.
  * Renomeação de colunas técnicas (D1N, V, etc.) para nomes legíveis (UF, Populacao).
  * Conversão de tipos (ex: Populacao de `string` para `double`).
* **Qualidade**: Implementação de **Data Quality Gates** para validar nulidade e valores inconsistentes (negativos).

###  Camada Gold (Consumo)
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

1. **Consistência de resultado**: O pipeline foi construído com o modo `overwrite`, permitindo que o processo seja executado múltiplas vezes sem gerar duplicidade de dados.
2. **Governança de Dados**: A inclusão de uma célula de **Data Quality** garante que falhas na API ou dados corrompidos não cheguem à camada Gold, protegendo a análise do usuário final.
3. **Escalabilidade**: Ao utilizar PySpark e Delta Lake, a solução está preparada para lidar com grandes volumes de dados (Big Data), indo além da simples manipulação de arquivos locais.
4. **Resiliência**: O uso de blocos de repetição (`for`) nas camadas permite que o pipeline seja facilmente expandido para outras tabelas do IBGE com poucas alterações de código.

---

## Evidência da estrutura construida
1. **Estrutura de pastas**

   **Portal azure**:
2.![Portal Azure ](https://github.com/tiago-arauj/dados-ibge/blob/main/assets/portal%20azure.png)

**Grupo de recurso**:
3. ![Grupo de recurso](https://github.com/tiago-arauj/dados-ibge/blob/main/assets/grupo%20de%20recurso.png)

**Azure databricks**:
4. ![Azure databricks](https://github.com/tiago-arauj/dados-ibge/blob/main/assets/azure%20databricks.png)

 **Containners**:
6. ![Containners](https://github.com/tiago-arauj/dados-ibge/blob/main/assets/containners.png)

1. **Camada Bronze**:

**Bronze população**:
2. ![Bronze populacao](https://github.com/tiago-arauj/dados-ibge/blob/main/assets/bronze_populacao.png)

 **Bronze população raw**:
3. ![Bronze populacao_raw](https://github.com/tiago-arauj/dados-ibge/blob/main/assets/bronze_populacao_raw.png)

   **Bronze população delta**:
4. ![Bronze populacao_raw](https://github.com/tiago-arauj/dados-ibge/blob/main/assets/bronze_populacao_raw_deltalog.png)
   
2. **Camada Silver**:

   **População silver**:
1.  ![Populacao_silver](https://github.com/tiago-arauj/dados-ibge/blob/main/assets/populacao%20silver.png)

 **População silver estrutura de pasta**:
2.  ![Populacao_silver](https://github.com/tiago-arauj/dados-ibge/blob/main/assets/populacao%20silver_estrutura.png)

   **População delta**:
3.  ![Populacao_silver_deltalog](https://github.com/tiago-arauj/dados-ibge/blob/main/assets/populacao%20silver_deltalog.png)
   
4. **Camada Gold**:

**Indicadores camada gold**:
1. ![Populacao_silver_deltalog](https://github.com/tiago-arauj/dados-ibge/blob/main/assets/gold_indicadores.png)

**ranking população uf**:
2. ![gold_ranking_populacao_uf](https://github.com/tiago-arauj/dados-ibge/blob/main/assets/gold_ranking_populacao_uf.png)

**População vs media regional**:
3. ![gold_ranking_populacao_uf](https://github.com/tiago-arauj/dados-ibge/blob/main/assets/gold_populacao_vs_media_regional.png)

**Representatividade Brasil**:
4. ![gold_ranking_populacao_uf](https://github.com/tiago-arauj/dados-ibge/blob/main/assets/gold_representatividade_brasil.png)

   


## Como Executar

1. Importe o notebook `.ipynb` presente neste repositório para o seu Workspace do Databricks.
2. Configure as credenciais de acesso da sua **Azure Storage Account** na célula de configuração inicial.
3. Execute todas as células para processar as camadas Bronze, Silver e Gold sequencialmente.
