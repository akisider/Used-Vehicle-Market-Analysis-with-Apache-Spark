from pyspark.sql.types import StructType

# ---------------------------------------------------------
# Task 3: Structured Streaming
# ---------------------------------------------------------

# Step 1: Create a directory for streaming input files

import os
os.makedirs("stream_input", exist_ok=True)

# Define the schema for the streaming data
input_schema = df.schema

# Step 2: Read the streaming data from the input directory

stream_df = spark.readStream \
    .schema(input_schema) \
    .option("header", "true") \
    .csv("stream_input")

# Step 3: Data Cleaning and Transformation


# Data Cleaning
stream_clean = stream_df \
    .withColumn("model", regexp_replace(col("model"), " ", "_")) \
    .withColumn("engine", regexp_replace(col("engine"), " ", "_")) \
    .withColumn("brand", upper(col("brand"))) \
    .withColumn("milage", regexp_replace(col("milage"), " mi\\.", "")) \
    .withColumn("milage", regexp_replace(col("milage"), ",", "").cast("double")) \
    .withColumn("price", regexp_replace(col("price"), "\\$", "")) \
    .withColumn("price", regexp_replace(col("price"), ",", "").cast("double"))

# Unit Conversions
stream_transformed = stream_clean \
    .withColumn("price", col("price") * 0.9) \
    .withColumn("milage", col("milage") * 1.60934) \
    .withColumn("value_index", col("milage") * col("price")) # Calculation of index for Requirement 2

# Step 4: Calculation of Statistics
streaming_stats = stream_transformed.groupBy("brand", "model").agg(
    avg("price").alias("average_price"),
    count("*").alias("vehicle_count"),
    min("value_index").alias("best_value_index")
)

# Step 5: Function for saving each batch to CSV
def save_to_csv(batch_df, batch_id):
    batch_df.coalesce(1).write \
        .csv("stream_output", header=True, mode="overwrite")

# Step 6: Start the Streaming
query = streaming_stats.writeStream \
    .outputMode("complete") \
    .foreachBatch(save_to_csv) \
    .start()

print("Streaming query started. Waiting for data...")