from pyspark.sql import SparkSession
from pyspark.sql.functions import stddev, avg, col, length

# Create a Spark session
spark = SparkSession.builder.appName('GCSFilesRead').getOrCreate()

# Set the GCS credentials path
keyfile_path = "C:/Users/HI/Downloads/task-data-404610-6c0225e1a962.json"
spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", keyfile_path)

# Specify GCS bucket and file path
bucket_name = "spark-task"
file_path = f"gs://{bucket_name}/text.csv"

# Read CSV file from GCS
df = spark.read.csv(file_path, header=True)
df.printSchema()

def calculate_stddev_avg():
    global df 
    column = "Color"
    df = df.withColumn("Color", df.Color.cast('int'))
    result = df.agg(stddev(column).alias("std"), avg(column).alias("avg"))
    result.show()

def calculate_frequency():
    global df
    string_column = "Beer_Style"
    frequency_df = df.groupBy(string_column).count()
    top_5_frequency = frequency_df.orderBy(col("count").desc()).limit(5)
    top_5_frequency.show()

def rename_column():
    global df
    old_column_name = "Beer_Style"
    new_column_name = "style"
    renamed_df = df.withColumnRenamed(old_column_name, new_column_name)
    renamed_df.show()

def calculate_max_min():
    global df
    df.select("Bitterness").summary("min", "25%", "75%", "max").show()

def calculate_length_of_characters():
    global df
    string_column = "Location"
    wordsLengths = df.select(length(string_column).alias('lengths'), string_column)
    wordsLengths.show()

calculate_stddev_avg()
calculate_frequency()
rename_column()
calculate_max_min()
calculate_length_of_characters()
