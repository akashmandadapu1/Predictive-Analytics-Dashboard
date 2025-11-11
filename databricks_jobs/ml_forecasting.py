# Databricks ML Regression Job (PySpark)
from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
import mlflow

# Start Spark session
spark = SparkSession.builder.appName("MLForecasting").getOrCreate()

# Load data
df = spark.read.csv("/dbfs/mnt/data/raw_metrics.csv", header=True, inferSchema=True)

feature_cols = [col for col in df.columns if col not in ('target','date', 'id')]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
df_assembled = assembler.transform(df)

# ML regression
lr = LinearRegression(featuresCol='features', labelCol='target')
model = lr.fit(df_assembled)

# Save model
mlflow.spark.log_model(model, "regression_model")
print("Model trained and logged with MLFlow")

# Predict and save results for PowerBI
predictions = model.transform(df_assembled)
results = predictions.select('date', 'prediction')
results.toPandas().to_csv("/dbfs/mnt/data/forecast_results.csv", index=False)
print("Forecast results exported")
