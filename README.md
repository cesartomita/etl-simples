
# Pipeline de Total de Vendas
Este projeto contém uma pipeline para processar arquivos JSON de vendas, calcular o total de vendas e exportar os resultados em diferentes formatos. A pipeline executa as seguintes etapas:

1. Extração e Consolidação: Lê e consolida dados de múltiplos arquivos JSON em um único DataFrame.

2. Cálculo de KPI: Calcula o total de vendas através da multiplicação da quantidade (quantity) pelo preço unitário (unit_price), criando uma nova coluna total_price.

3. Exportação dos Dados: Exporta os dados processados nos formatos CSV, Excel e Parquet, conforme especificado.

## Como executar

Executar o arquivo `pipeline.py`:

```
    python pipeline.py
```