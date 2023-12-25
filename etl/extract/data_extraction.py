"""
data_extraction.py
~~~~~~~~~~~~~~~~~~

extracte é essencial para nosso pipeline de dados, realizando ETL para extrair e preparar dados de fontes diversas. 
Executado como job em um cluster, otimiza o processamento distribuído para lidar com grandes conjuntos de dados. 
Este packager encapsula funcionalidades críticas, contribuindo para a coesão e qualidade dos dados. 
Foco na modularidade e eficiência, com o objetivo de fornecer insights valiosos. 
O processo concentra-se na extração de dados de uma localização genérica, promovendo um código claro e conciso.
"""

def print_cvs(df):
    print(df)


def extracao_csv(spark_session, caminho_arquivo):
    """
    Extrai dados de um arquivo CSV usando Apache Spark.

    Parameters:
    - spark_session: A sessão Spark.
    - caminho_arquivo (str): O caminho do arquivo CSV.

    Returns:
    - DataFrame: Um DataFrame Spark representando os dados do CSV.
    """
    # Leia os dados do CSV para um DataFrame Spark
    dados_spark = spark_session.read.csv(caminho_arquivo, header=True, inferSchema=True)

    return dados_spark
