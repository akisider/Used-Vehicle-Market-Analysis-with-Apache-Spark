from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, min

from cleaning import RAW_SCHEMA, clean_vehicles, count_nulls, to_metric

# Spark Session
spark = SparkSession.builder \
    .appName("CarDataAnalysis_Assignment") \
    .getOrCreate()

# Load the raw data with an explicit schema (price and mileage as text)
raw_df = spark.read.csv("used_cars.csv", header=True, schema=RAW_SCHEMA)

# ---------------------------------------------------------
# Task 1: Cleaning and transformation
# ---------------------------------------------------------

# Steps 1-2 and 4.1-4.2: shared cleaning (brand, model, engine, mileage, price)
df = clean_vehicles(raw_df)

# Data quality check: casting text to numbers fails silently (it returns null),
# so compare null counts before and after cleaning
checked_columns = ["milage", "price", "fuel_type"]
nulls_before = count_nulls(raw_df, checked_columns)
nulls_after = count_nulls(df, checked_columns)

print(f"Rows loaded: {raw_df.count()}")
for c in checked_columns:
    print(f"Nulls in {c}: before cleaning {nulls_before[c]}, after cleaning {nulls_after[c]}")

for c in ["milage", "price"]:
    if nulls_after[c] > nulls_before[c]:
        print(f"WARNING: {nulls_after[c] - nulls_before[c]} values in '{c}' could not be converted to numbers")

# Step 3: Count the number of Audi vehicles from 2016 to 2020
audi_count = df.filter(
    (col("brand") == "AUDI") &
    (col("model_year") >= 2016) &
    (col("model_year") <= 2020)
).count()

print(f"Number of Audi vehicles (2016-2020): {audi_count}")

# Step 4.3: Add price in EUR and mileage in km (original columns are kept)
df_transformed = to_metric(df)

# Step 4.4: Save the transformed DataFrame to CSV
df_transformed.coalesce(1).write.csv("transformed_vehicles_output", header=True, mode="overwrite")

# Step 5: Count the number of vehicles for each brand and save to CSV
brand_counts = df_transformed.groupBy("brand").count()
brand_counts.coalesce(1).write.csv("brand_counts_output", header=True, mode="overwrite")

# ---------------------------------------------------------
# Task 2: Statistics table per brand and model
# ---------------------------------------------------------

# Value index as defined in the assignment: mileage (km) * price (EUR)
df_stats_input = df_transformed.withColumn("value_index", col("milage_km") * col("price_eur"))

stats_table = df_stats_input.groupBy("brand", "model").agg(
    avg("price_eur").alias("average_price"),       # 1. Μέσο κόστος
    count("*").alias("vehicle_count"),             # 2. Πλήθος οχημάτων
    min("value_index").alias("best_value_index")   # 3. Χαμηλότερος δείκτης αξίας
)

print("Sample:")
stats_table.show()

stats_table.coalesce(1).write.csv("statistics_output", header=True, mode="overwrite")

print("The statistics table has been saved to the folder 'statistics_output'")
