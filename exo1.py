from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, max

spark = SparkSession.builder.appName("Analyse des ventes").getOrCreate()
ventes_df = spark.read.csv("ventes.csv", header=True, inferSchema=True)
ventes_df = ventes_df.withColumn("Chiffre_affaires", col("Quantité") * col("Prix_unitaire"))
chiffre_affaires_total = ventes_df.agg(sum("Chiffre_affaires").alias("Chiffre_affaires_total")).collect()[0][0]
produit_le_plus_vendu = ventes_df.groupBy("Produit").agg(sum("Quantité").alias("Total_quantité")) \
    .orderBy(col("Total_quantité").desc()).first()

print(f"Chiffre d'affaires total : {chiffre_affaires_total} €")
print(f"Produit le plus vendu : {produit_le_plus_vendu['Produit']} ({produit_le_plus_vendu['Total_quantité']} unités)")

