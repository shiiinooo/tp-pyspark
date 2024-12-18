from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Produits les plus chers") \
    .master("local[*]") \
    .getOrCreate()

fichier_csv = "produits.csv"
produits_df = spark.read.csv(fichier_csv, header=True, inferSchema=True)

print("Catalogue des produits :")
produits_df.show()

produits_les_plus_chers = produits_df.orderBy("Prix", ascending=False).limit(3)

print("Les 3 produits les plus chers :")
produits_les_plus_chers.show()

spark.stop()

