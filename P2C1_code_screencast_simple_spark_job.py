from pyspark.sql import SparkSession

# Configuration de Spark
spark = SparkSession.builder \
    .appName("SimpleSparkJob") \
    .master("local[2]") \
    .getOrCreate()

# Création d'un RDD de paires clé-valeur
data = spark.sparkContext.parallelize([(i, i*10) for i in range(1000)], 4)  # 4 partitions

# Transformation étroite : Filtrer les paires avec des clés paires
filtered_data = data.filter(lambda x: x[0] % 2 == 0)

# Transformation large : Réduire par clé (nécessite un shuffle)
reduced_data = filtered_data.reduceByKey(lambda x, y: x + y)

# Action : Collecter les résultats
result = reduced_data.collect()

print(f"Résultats : {result}")

# Affichage du lien vers l'interface web de Spark pour visualiser le DAG
print("Ouvrez votre navigateur et accédez à http://localhost:4040 pour visualiser le DAG.")

# Gardez la session Spark active pour explorer l'interface web
input("Appuyez sur Entrée pour arrêter la session Spark...")

# Arrêt de la session Spark
spark.stop()