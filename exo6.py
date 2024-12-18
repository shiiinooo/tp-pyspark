from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum

spark = SparkSession.builder \
    .appName("Agrégation par catégorie") \
    .master("local[*]") \
    .getOrCreate()

fichier_csv = "produits.csv"
produits_df = spark.read.csv(fichier_csv, header=True, inferSchema=True)

print("Produits :")
produits_df.show()

resultat_aggregation_df = produits_df.groupBy("Catégorie") \
    .agg(
        avg("Prix").alias("Prix_moyen"),
        sum("Prix").alias("Prix_total")
    )

print("Prix moyen et prix total par catégorie :")
resultat_aggregation_df.show()

spark.stop()


