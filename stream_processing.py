import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, min

from cleaning import RAW_SCHEMA, clean_vehicles, to_metric

# ---------------------------------------------------------
# Task 3: Structured Streaming
# ---------------------------------------------------------

spark = SparkSession.builder \
    .appName("CarDataAnalysis_Streaming") \
    .getOrCreate()

# Step 1: Create a directory for streaming input files
os.makedirs("stream_input", exist_ok=True)

# Step 2: Read the streaming data from the input directory.
# The stream uses the RAW schema (price and mileage as text). Using the schema
# of the already-cleaned batch DataFrame would make Spark read "51,000 mi."
# as a number, which silently returns null.
stream_df = spark.readStream \
    .schema(RAW_SCHEMA) \
    .option("header", "true") \
    .csv("stream_input")

# Step 3: Same cleaning and unit conversion as the batch job
stream_transformed = to_metric(clean_vehicles(stream_df)) \
    .withColumn("value_index", col("milage_km") * col("price_eur"))

# Step 4: Calculation of statistics
streaming_stats = stream_transformed.groupBy("brand", "model").agg(
    avg("price_eur").alias("average_price"),
    count("*").alias("vehicle_count"),
    min("value_index").alias("best_value_index")
)


# Step 5: Function for saving each batch to CSV
def save_to_csv(batch_df, batch_id):
    batch_df.coalesce(1).write \
        .csv("stream_output", header=True, mode="overwrite")


# Step 6: Start the streaming query
query = streaming_stats.writeStream \
    .outputMode("complete") \
    .foreachBatch(save_to_csv) \
    .start()

print("Streaming query started. Waiting for data...")
query.awaitTermination()
