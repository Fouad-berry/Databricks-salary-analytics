# Databricks Salary Analytics


Projet Data Lakehouse complet avec Databricks

**Points forts :**
- 250 000 lignes (Big Data crédible)
- Données structurées, variables business, target (salary)
- Pipeline professionnel : ingestion, nettoyage, analytics, ML
- Valorisation CV : Databricks, PySpark, Data Lakehouse, ETL, ML

databricks-salary-analytics/
│
├── notebooks/
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_cleaning.py
│   ├── 03_gold_aggregation.py
│   ├── 04_salary_prediction.py
│
├── data/
├── README.md

## Structure du projet

```
databricks-salary-analytics/
│
├── notebooks/
│   ├── 01_bronze_ingestion.py   # Ingestion brute (Bronze)
│   ├── 02_silver_cleaning.py    # Nettoyage & typage (Silver)
│   ├── 03_gold_aggregation.py   # Agrégations analytiques (Gold)
│   ├── 04_salary_prediction.py  # Prédiction de salaire (ML)
│
├── data/                       # Données sources
├── README.md                   # Documentation
```


## Objectif
Construire un pipeline Data Lakehouse complet pour l’analyse et la prédiction de salaires avec Databricks.


## Pipeline & Étapes

### 1. 🟤 Bronze Layer (Raw)
**But :** Ingestion des données brutes
*Charger le CSV dans Databricks et stocker tel quel (Delta)*

```python
df = spark.read.csv("/FileStore/job_salary.csv", header=True, inferSchema=True)
df.write.format("delta").save("/mnt/bronze/jobs")
```

### 2. ⚪ Silver Layer (Cleaned)
**But :** Nettoyage et typage
*Suppression des nulls, typage, normalisation*

```python
df = spark.read.format("delta").load("/mnt/bronze/jobs")
df_clean = df.dropna()
df_clean.write.format("delta").mode("overwrite").save("/mnt/silver/jobs")
```

### 3. 🟡 Gold Layer (Analytics)
**But :** Agrégations business
*Salaire moyen par job, pays, expérience, etc.*

```python
df = spark.read.format("delta").load("/mnt/silver/jobs")
df_gold = df.groupBy("job_title").agg({"salary": "avg"})
df_gold.write.format("delta").mode("overwrite").save("/mnt/gold/jobs")
```

### 4. 🔥 Bonus ML (Prédiction)
**But :** Prédire le salaire à partir des variables business

```python
# Pipeline ML avec PySpark (RandomForestRegressor)
# Features : job_title, country, experience_level, education
# Target : salary
```


---

## Analyses possibles
- Salaire moyen par job
- Salaire vs expérience
- Salaire vs éducation
- Remote vs salary

## Valorisation en entretien
> "J’ai construit un pipeline Data Lakehouse complet sur Databricks (Bronze/Silver/Gold) avec ingestion, nettoyage, analytics et prédiction de salaire."

---

Prêt à démarrer !
