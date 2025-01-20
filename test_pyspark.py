from pyspark.sql import SparkSession

# Créer une session Spark
spark = SparkSession.builder.appName("TestPySpark").getOrCreate()

# Créer un DataFrame simple
data = [("Alice", 1), ("Bob", 2), ("Cathy", 3)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Afficher le DataFrame
df.show()
