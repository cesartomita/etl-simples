import pandas as pd
import os
import glob

def extrair_dados_e_consolidar(pasta: str) -> pd.DataFrame:
    df_list = list()
    arquivos_json = glob.glob(os.path.join(pasta, "*.json"))
    for arquivo in arquivos_json:
        df = pd.read_json(arquivo)
        df_list.append(df)
    df_total = pd.concat(df_list, ignore_index=True)
    # ignore_index=True: Essa opção redefine o índice no DataFrame concatenado, de forma que os índices anteriores de cada DataFrame sejam ignorados e substituídos por um novo índice contínuo.
    # Isso é útil se você quiser um índice sequencial no DataFrame final.
    return df_total

def calcular_kpi_de_total_de_vendas(df: pd.DataFrame) -> pd.DataFrame:
    df["total_price"] = df["quantity"] * df["unit_price"]
    return df

def carregar_dados(df: pd.DataFrame, formato_saida: list):
    """
    formato_saida: pode ser csv, excel ou parquet
    """
    for formato in formato_saida:
        if formato == "csv":
            df.to_csv("./export/export.csv")
            print("Exportado dados em CSV")
        if formato == "excel":
            df.to_excel("./export/export.xlsx")
            print("Exportado dados em Excel")
        if formato == "parquet":
            df.to_parquet("./export/export.parquet")
            print("Exportado dados em Parquet")

def pipeline_total_vendas(caminho_pasta, formato_saida):
    """
    Calcula o total de vendas
    caminho_pasta: caminho da pasta dos arquivos json
    formato_saida: pode ser csv, excel ou parquet
    """
    print("pipeline_total_vendas")
    df_dados = extrair_dados_e_consolidar(caminho_pasta)
    df_total = calcular_kpi_de_total_de_vendas(df_dados)
    carregar_dados(df_total, formato_saida)

if __name__ == "__main__":
    print("__name__")
    pasta = "./data"
    df_dados = extrair_dados_e_consolidar(pasta)
    df_total = calcular_kpi_de_total_de_vendas(df_dados)
    formato_export = ["csv", "excel", "parquet"]
    carregar_dados(df_total, formato_export)
