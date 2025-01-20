from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum
import random
import time
# Start measuring time
start_time = time.time()

# Configuration de Spark
spark = SparkSession.builder \
    .appName("SimpleSparkJob") \
    .master("local[2]") \
    .getOrCreate()

# Création d'un DataFrame à partir d'une liste de tuples
data = [(random.randint(0, 10), random.randint(10, 10000)) for _ in range(1000000)]
df = spark.createDataFrame(data, schema=["key", "value"])

# Repartition the DataFrame into 4 partitions
df = df.repartition(4)

# Check the number of partitions
print(f"Number of partitions: {df.rdd.getNumPartitions()}")

# Partition sizes
partition_sizes = df.rdd.glom().map(len).collect()
print(f"Size of each partition: {partition_sizes}")

# Transformation étroite : Filtrer les paires avec des clés paires
filtered_df = df.filter(col("key") % 2 == 0)

# Transformation large : Réduire par clé (nécessite un shuffle)
# Ici, nous utilisons groupBy et agg pour effectuer une agrégation
reduced_df = filtered_df.groupBy("key").agg(sum("value").alias("total_value"))

# Action : Collecter les résultats
result = reduced_df.collect()

# Afficher les résultats
print("Résultats :")
for row in result:
    print(f"Clé : {row['key']}, Valeur totale : {row['total_value']}")

# End measuring time
end_time = time.time()
execution_time = end_time - start_time
print(f"Execution time: {execution_time} seconds")

# Affichage du lien vers l'interface web de Spark pour visualiser le DAG
print("Ouvrez votre navigateur et accédez à http://localhost:4040 pour visualiser le DAG.")

# Gardez la session Spark active pour explorer l'interface web
input("Appuyez sur Entrée pour arrêter la session Spark...")

# Arrêt de la session Spark
spark.stop()