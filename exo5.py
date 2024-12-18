from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum

spark = SparkSession.builder \
    .appName("Analyse des transactions") \
    .master("local[*]") \
    .getOrCreate()

fichier_csv = "transactions.csv"
transactions_df = spark.read.csv(fichier_csv, header=True, inferSchema=True)


print("Transactions :")
transactions_df.show()

depenses_totales_df = transactions_df.groupBy("Client").agg(_sum("Montant").alias("Dépenses_totales"))

depenses_totales_df = depenses_totales_df.orderBy("Dépenses_totales", ascending=False)

print("Dépenses totales par client :")
depenses_totales_df.show()

spark.stop()

