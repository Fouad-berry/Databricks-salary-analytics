# SILVER LAYER - Nettoyage des données

# Databricks notebook source

# 1. Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower

# 2. Spark session
spark = SparkSession.builder.appName("SilverCleaning").getOrCreate()

# 3. Lecture Bronze
df = spark.read.format("delta").load("/mnt/bronze/jobs")

print("🔍 Schema initial")
df.printSchema()

# 4. Nettoyage des nulls (colonnes critiques uniquement 🔥)
df_clean = df.dropna(subset=["salary", "job_title", "experience_years"])

# 5. Nettoyage texte (très important en prod 🔥)
df_clean = df_clean.withColumn("job_title", trim(lower(col("job_title"))))
df_clean = df_clean.withColumn("industry", trim(lower(col("industry"))))
df_clean = df_clean.withColumn("education_level", trim(lower(col("education_level"))))

# 6. Typage propre
df_clean = df_clean.withColumn("salary", col("salary").cast("double"))
df_clean = df_clean.withColumn("experience_years", col("experience_years").cast("int"))
df_clean = df_clean.withColumn("skills_count", col("skills_count").cast("int"))
df_clean = df_clean.withColumn("certifications", col("certifications").cast("int"))

# 7. Suppression des doublons
df_clean = df_clean.dropDuplicates()

# 8. Feature engineering (🔥 niveau +)
df_clean = df_clean.withColumn(
    "experience_level",
    col("experience_years") / 5
)

# 9. Vérification
print("✅ Schema après nettoyage")
df_clean.printSchema()

print("📊 Aperçu des données nettoyées")
df_clean.show(5)

# 10. Sauvegarde Silver
df_clean.write.format("delta").mode("overwrite").save("/mnt/silver/jobs")

print("🎉 Silver layer terminé avec succès !")