from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("Analyse des utilisateurs") \
    .master("local[*]") \
    .getOrCreate()

utilisateurs_df = spark.read.json("utilisateurs.json", multiLine=True)
utilisateurs_df.show()

age_moyen = utilisateurs_df.agg({"âge": "avg"}).collect()[0][0]

utilisateurs_par_ville = utilisateurs_df.groupBy("ville").count()

plus_jeune_utilisateur = utilisateurs_df.orderBy("âge").limit(1).collect()[0]

print(f"Âge moyen : {age_moyen:.1f} ans\n")
print("Nombre d'utilisateurs par ville :")
utilisateurs_par_ville.show()

print("\nPlus jeune utilisateur :")
print(f"{plus_jeune_utilisateur['nom']} ({plus_jeune_utilisateur['âge']} ans)")


spark.stop()

