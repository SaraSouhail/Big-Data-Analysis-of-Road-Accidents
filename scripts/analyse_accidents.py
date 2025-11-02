from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# 1. Spark session
spark = SparkSession.builder.appName("AccidentsAnalyse").getOrCreate()
print("Step 1: Spark session created.")

# 2. Load CSV
df = spark.read.csv("../dataset/US_Accidents.csv", header=True, inferSchema=True)
print(f"Step 2: Loaded CSV with {df.count()} rows and {len(df.columns)} columns.")

# 3. Clean data
df_clean = df.dropna(subset=["Severity", "Start_Time", "City"]) \
             .filter(col("Severity").isin([1, 2, 3, 4])) \
             .dropDuplicates()
print(f"Step 3: Data cleaned. Rows after cleaning: {df_clean.count()}.")

# 4. Feature engineering
df_clean = df_clean.withColumn("Hour", hour("Start_Time"))

# Ensure no null in categorical column
df_clean = df_clean.filter(col("Weather_Condition").isNotNull())

# StringIndexer with handleInvalid
indexer = StringIndexer(
    inputCol="Weather_Condition",
    outputCol="WeatherIndex",
    handleInvalid="skip"  # or "keep"
)
df_encoded = indexer.fit(df_clean).transform(df_clean)

# Drop rows with nulls in numerical feature columns
df_encoded = df_encoded.dropna(subset=["Temperature(F)", "Humidity(%)", "Visibility(mi)"])

# VectorAssembler to build features vector
assembler = VectorAssembler(
    inputCols=["Temperature(F)", "Humidity(%)", "Visibility(mi)", "Hour", "WeatherIndex"],
    outputCol="features"
)
df_final = assembler.transform(df_encoded).select("features", "Severity")
print("Step 4: Feature engineering completed. Preview of processed data:")
df_final.show(5, truncate=False)

# 5. Split data and train Random Forest model
train, test = df_final.randomSplit([0.8, 0.2], seed=42)
print(f"Step 5: Data split into train ({train.count()}) and test ({test.count()}) sets.")

model = RandomForestClassifier(labelCol="Severity", featuresCol="features")
rf_model = model.fit(train)
print("Step 5: Random Forest model training complete.")
rf_model.save("models/random_forest_accident_model")
print("Step 5: Model saved to 'models/random_forest_accident_model'.")

predictions = rf_model.transform(test)
print("Step 5: Predictions made on test set.")
predictions.select("features", "Severity", "prediction").show(5)

# 6. Evaluate model
evaluator = MulticlassClassificationEvaluator(labelCol="Severity", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print(f"Step 6: Model accuracy on test set: {accuracy:.4f}")

# Optional: show confusion matrix
print("Confusion Matrix (Severity vs Prediction):")
predictions.groupBy("Severity", "prediction").count().show()
