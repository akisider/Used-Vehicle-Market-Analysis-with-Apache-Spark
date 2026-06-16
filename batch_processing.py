

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, upper

# Spark Session
spark = SparkSession.builder \
    .appName("CarDataAnalysis_Assignment") \
    .getOrCreate()

# Load data from CSV 
df = spark.read.csv("used_cars.csv", header=True, inferSchema=True)


# Step 1: Replace spaces with underscores in 'model' and 'engine' columns

df = df.withColumn("model", regexp_replace(col("model"), " ", "_")) \
       .withColumn("engine", regexp_replace(col("engine"), " ", "_"))

# Step 2: Convert 'brand' column to uppercase

df = df.withColumn("brand", upper(col("brand")))


# Step 3: Count the number of Audi vehicles from 2016 to 2020

audi_count = df.filter(
    (col("brand") == "AUDI") &
    (col("model_year") >= 2016) &
    (col("model_year") <= 2020)
).count()

print(f"Number of Audi vehicles (2016-2020): {audi_count}")

# Step 4: Data Cleaning and Transformation


# 1. Cleaning the 'milage' column
# We remove " mi." and "," and convert it to a number (Double)
df = df.withColumn("milage", regexp_replace(col("milage"), " mi\\.", "")) \
       .withColumn("milage", regexp_replace(col("milage"), ",", "")) \
       .withColumn("milage", col("milage").cast("double"))

# 2. Cleaning the 'price' column
# We remove "$" and "," and convert it to a number (Double)
df = df.withColumn("price", regexp_replace(col("price"), "\\$", "")) \
       .withColumn("price", regexp_replace(col("price"), ",", "")) \
       .withColumn("price", col("price").cast("double"))

# 3. Transformation: Convert 'milage' from miles to kilometers (1 mile = 1.60934 km)
df_transformed = df.withColumn("price", col("price") * 0.9) \
                   .withColumn("milage", col("milage") * 1.60934)

# 4. Save the transformed DataFrame to CSV
df_transformed.coalesce(1).write.csv("transformed_vehicles_output", header=True, mode="overwrite")

# Step 5: Count the number of vehicles for each brand and save to CSV

brand_counts = df_transformed.groupBy("brand").count()

# Save the brand counts to CSV
brand_counts.coalesce(1).write.csv("brand_counts_output", header=True, mode="overwrite")


from pyspark.sql.functions import avg, count, min, col


# Task 2: Create a statistics table with average price, vehicle count, and best value index for each brand and model


# Step 1: Create a new column for the value index
df_stats_input = df_transformed.withColumn("value_index", col("milage") * col("price"))

# Step 2: Group by "brand" and "model" simultaneously and calculate the required statistics
stats_table = df_stats_input.groupBy("brand", "model").agg(
    avg("price").alias("average_price"),          # 1. Μέσο κόστος 
    count("*").alias("vehicle_count"),            # 2. Πλήθος οχημάτων 
    min("value_index").alias("best_value_index")  # 3. Χαμηλότερος δείκτης αξίας 
)

# Display the statistics table sample
print("Sample:")
stats_table.show()

# Step 3: Save the statistics table to CSV
stats_table.coalesce(1).write.csv("statistics_output", header=True, mode="overwrite")

print("The statistics table has been saved to the folder 'statistics_output'")