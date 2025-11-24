from pyspark.ml.pipeline import PipelineModel

import os

os.environ["PYSPARK_PYTHON"] = r"C:\Users\Lenovo\miniconda3\envs\pyspark_3.10\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\Lenovo\miniconda3\envs\pyspark_3.10\python.exe"

PipelineModel.load("D:/PyCharm/HUST/BigData/MainProject/BigData-20251/MLModels/Model/DT")
