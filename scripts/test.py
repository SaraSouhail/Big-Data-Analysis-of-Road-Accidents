from pyspark.sql import SparkSession

# 1. Créer une session Spark
spark = SparkSession.builder \
    .appName("AccidentsAnalyse") \
    .getOrCreate()

# 2. Charger le dataset
df = spark.read.csv("../dataset/US_Accidents.csv", header=True, inferSchema=True)

# 3. ✅ Vérifier que le dataset est bien chargé
print("✅ Dataset chargé avec succès")
df.show(5)              # 👉 Affiche les 5 premières lignes
df.printSchema()        # 👉 Affiche le type des colonnes
print("Nombre de lignes :", df.count())  # 👉 Nombre total de lignes
print("Colonnes :", df.columns)          # 👉 Liste des colonnes

# 4. Fermer proprement Spark
spark.stop()
