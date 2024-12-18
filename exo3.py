from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, col, lit

spark = SparkSession.builder \
    .appName("Nettoyage de données") \
    .master("local[*]") \
    .getOrCreate()

fichier_csv = "clients.csv"
clients_df = spark.read.csv(fichier_csv, header=True, inferSchema=True)

print("Données originales :")
clients_df.show()

moyenne_age = clients_df.select(mean(col("Âge"))).collect()[0][0]
clients_df = clients_df.fillna({"Âge": moyenne_age})

clients_df = clients_df.fillna({"Ville": "Inconnue"})

clients_df = clients_df.na.drop(subset=["Revenu"])

print("Données nettoyées :")
clients_df.show()

spark.stop()

