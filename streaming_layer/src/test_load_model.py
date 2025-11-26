from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
import os

os.environ["PYSPARK_PYTHON"] = r"C:\Users\Lenovo\miniconda3\envs\pyspark_3.10\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\Lenovo\miniconda3\envs\pyspark_3.10\python.exe"

spark = SparkSession.builder \
    .appName("LoadModel") \
    .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
    .config("spark.hadoop.fs.local.block.size", "33554432") \
    .getOrCreate()

model_path = r"../../MLModels/Model/DT"
model = PipelineModel.load(model_path)

print("Model loaded successfully!")
