import os
import sys
from pyspark.sql import SparkSession


# Set the Python executable for PySpark
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Create a Spark session
spark = SparkSession.builder.getOrCreate()

# Create the DataFrame
df = spark.createDataFrame([("A", 1, None), ("B", None, "123"), ("B", 3, "456"), ("D", None, None)],["Name", "Value", "id"])

result_df = df.na.drop(subset=["Name"])

# Show the result
result_df.show()