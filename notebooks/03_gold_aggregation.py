# GOLD LAYER - Analytics (VERSION PRO)

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, sum, round

# Spark session
spark = SparkSession.builder.appName("GoldAggregation").getOrCreate()

# Lecture Silver
df = spark.read.format("delta").load(
    "/Volumes/workspace/default/data_ingestion/silver_jobs"
)

print("🔍 Schema Silver")
df.printSchema()

# =========================
# 📊 1. SALAIRE PAR JOB
# =========================
df_job = df.groupBy("job_title").agg(
    round(avg("salary"), 2).alias("avg_salary"),
    count("*").alias("total_jobs")
).orderBy("avg_salary", ascending=False)

# =========================
# 🌍 2. SALAIRE PAR LOCATION
# =========================
df_location = df.groupBy("location").agg(
    round(avg("salary"), 2).alias("avg_salary"),
    count("*").alias("total_jobs")
).orderBy("avg_salary", ascending=False)

# =========================
# 🎓 3. SALAIRE PAR EDUCATION
# =========================
df_education = df.groupBy("education_level").agg(
    round(avg("salary"), 2).alias("avg_salary"),
    count("*").alias("total_jobs")
).orderBy("avg_salary", ascending=False)

# =========================
# 📈 4. SALAIRE PAR EXPERIENCE
# =========================
df_experience = df.groupBy("experience_years").agg(
    round(avg("salary"), 2).alias("avg_salary"),
    count("*").alias("total_jobs")
).orderBy("experience_years")

# =========================
# 🏢 5. SALAIRE PAR INDUSTRY
# =========================
df_industry = df.groupBy("industry").agg(
    round(avg("salary"), 2).alias("avg_salary"),
    count("*").alias("total_jobs")
).orderBy("avg_salary", ascending=False)

# =========================
# 🏠 6. REMOTE VS NON REMOTE
# =========================
df_remote = df.groupBy("remote_work").agg(
    round(avg("salary"), 2).alias("avg_salary"),
    count("*").alias("total_jobs")
)

# =========================
# 💥 7. KPI GLOBAL
# =========================
df_kpi = df.agg(
    round(avg("salary"), 2).alias("global_avg_salary"),
    count("*").alias("total_records")
)

# =========================
# 💾 SAVE GOLD TABLES
# =========================

# SAVE GOLD
df_job.write.format("delta").mode("overwrite").save(
    "/Volumes/workspace/default/data_ingestion/gold_jobs_by_title"
)

df_location.write.format("delta").mode("overwrite").save(
    "/Volumes/workspace/default/data_ingestion/gold_jobs_by_location"
)

df_education.write.format("delta").mode("overwrite").save(
    "/Volumes/workspace/default/data_ingestion/gold_jobs_by_education"
)

df_experience.write.format("delta").mode("overwrite").save(
    "/Volumes/workspace/default/data_ingestion/gold_jobs_by_experience"
)

df_industry.write.format("delta").mode("overwrite").save(
    "/Volumes/workspace/default/data_ingestion/gold_jobs_by_industry"
)

df_remote.write.format("delta").mode("overwrite").save(
    "/Volumes/workspace/default/data_ingestion/gold_jobs_remote_analysis"
)

df_kpi.write.format("delta").mode("overwrite").save(
    "/Volumes/workspace/default/data_ingestion/gold_global_kpis"
)
print("🎉 Gold layer terminé (niveau pro) !")