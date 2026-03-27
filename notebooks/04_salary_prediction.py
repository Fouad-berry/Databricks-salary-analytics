# BONUS ML - Salary Prediction (VERSION PRO)

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator

# Spark
spark = SparkSession.builder.appName("SalaryPrediction").getOrCreate()

# Load data
df = spark.read.format("delta").load("/mnt/silver/jobs")

# Nettoyage
df = df.dropna(subset=["salary", "job_title", "education_level", "experience_years"])

# =========================
# 🔄 FEATURE ENGINEERING
# =========================

# Encodage variables catégorielles
categorical_cols = ["job_title", "education_level", "industry", "location"]

indexers = [
    StringIndexer(inputCol=col, outputCol=col + "_idx", handleInvalid="keep")
    for col in categorical_cols
]

# Features numériques + encodées
feature_cols = [
    "experience_years",
    "skills_count",
    "certifications"
] + [col + "_idx" for col in categorical_cols]

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# =========================
# 🌲 MODELE
# =========================

rf = RandomForestRegressor(
    featuresCol="features",
    labelCol="salary",
    numTrees=50,
    maxDepth=10
)

pipeline = Pipeline(stages=indexers + [assembler, rf])

# =========================
# 📊 SPLIT
# =========================

train, test = df.randomSplit([0.8, 0.2], seed=42)

# =========================
# 🚀 TRAIN
# =========================

model = pipeline.fit(train)

# =========================
# 🔮 PREDICTION
# =========================

predictions = model.transform(test)

# =========================
# 📏 EVALUATION
# =========================

rmse_eval = RegressionEvaluator(labelCol="salary", predictionCol="prediction", metricName="rmse")
mae_eval = RegressionEvaluator(labelCol="salary", predictionCol="prediction", metricName="mae")
r2_eval = RegressionEvaluator(labelCol="salary", predictionCol="prediction", metricName="r2")

print(f"📉 RMSE: {rmse_eval.evaluate(predictions):.2f}")
print(f"📉 MAE: {mae_eval.evaluate(predictions):.2f}")
print(f"📊 R2: {r2_eval.evaluate(predictions):.2f}")

# =========================
# 💾 SAVE MODEL
# =========================

model.write().overwrite().save("/mnt/models/salary_rf")

print("🎉 Modèle entraîné et sauvegardé avec succès !")