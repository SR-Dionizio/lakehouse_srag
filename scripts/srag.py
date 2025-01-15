from pyspark.sql import SparkSession
import sys
import os

# Adicionar o caminho desejado ao PYTHONPATH
caminho_desejado = '/caminho/para/seu/projeto'
if caminho_desejado not in sys.path:
    sys.path.append(caminho_desejado)

# Inicia o Spark
spark = SparkSession.builder \
    .appName("MeuApp") \
    .master("local[*]") \
    .getOrCreate()

# Leitura de um arquivo CSV
df = spark.read.csv('data/bronze/', header=True, inferSchema=True)
df.show()

spark.stop()
