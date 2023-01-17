from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, udf
from pyspark.sql.types import StringType
import uuid

# Create a SparkSession
spark = SparkSession.builder.appName("IngestCSVToDeltaLake").getOrCreate()

# Define a function to generate the batch_id column
def generate_batch_id():
    return str(uuid.uuid4())

# Register the UDF
spark.udf.register("generate_batch_id", generate_batch_id, StringType())

# Define the input and output paths
input_path = "D:/Proxy_Work/IngestCSVDeltaLake"
delta_path = "D:/Proxy_Work/IngestCSVDeltaLaketable"

# Read in the CSV files
df = spark.read.format("csv").option("header", "true").load(input_path)

# Add the ingestion_tms and batch_id columns
df = df.withColumn("ingestion_tms", current_timestamp())
df = df.withColumn("batch_id", generate_batch_id())

# Write the DataFrame to the Delta Lake table using the APPEND mode
df.write.format("delta").mode("append").save(delta_path)

# Stop the SparkSession
spark.stop()
