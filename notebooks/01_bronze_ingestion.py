# BRONZE LAYER - Ingestion des données brutes
# Databricks notebook source

# 1. Import des librairies
from pyspark.sql import SparkSession

# 2. Création de la session Spark
spark = SparkSession.builder.appName("BronzeIngestion").getOrCreate()

# 3. Lecture du fichier CSV brut
df = spark.read.csv("/FileStore/job_salary.csv", header=True, inferSchema=True)

df.printSchema()
df.show(5)

# 4. Sauvegarde au format Delta (Bronze)
df.write.format("delta").mode("overwrite").save("/mnt/bronze/jobs")

print("Ingestion Bronze terminée !")
