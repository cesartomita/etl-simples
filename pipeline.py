from etl import pipeline_total_vendas

pasta_json = "./data"
formato_saida = ["csv", "excel", "parquet"]

pipeline_total_vendas(pasta_json, formato_saida)