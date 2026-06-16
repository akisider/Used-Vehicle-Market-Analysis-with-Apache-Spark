# Used Vehicle Market Analysis with Apache Spark

## Project Overview
This project demonstrates a complete data engineering pipeline using **Apache Spark (PySpark)** to process, clean, and analyze a large-scale dataset of the secondary automotive market. It includes both Batch Processing and Structured Streaming implementations.

## Tech Stack
* **Language:** Python
* **Data Processing:** Apache Spark (PySpark), Spark SQL
* **Data Format:** CSV, DataFrames

## Key Features & Pipeline Steps

### 1. Batch Processing (`batch_processing.py`)
* **Data Ingestion:** Loaded the `used_cars.csv` dataset.
* **Data Cleansing:** Standardized string formats (uppercase conversions, replacing spaces with underscores in models/engines).
* **Data Type Casting:** Handled nulls and converted dirty string metrics (e.g., `$51,000`, `51,000 mi.`) into clean `DoubleType` numerical values using Regex (`regexp_replace`).
* **Feature Engineering:** Created new business metrics, such as a "Value Index" (Mileage * Price).
* **Aggregation:** Grouped data to extract market statistics (e.g., average prices, vehicle counts per brand).

### 2. Structured Streaming (`stream_processing.py`)
* Designed a real-time streaming architecture monitoring an input directory.
* Applied the exact same data validation and transformation logic to incoming micro-batches.
* Calculated real-time moving averages and vehicle counts.

## Challenges Solved
* **Type Casting Errors:** Handled `NumberFormatException` issues by proactively stripping non-numeric characters (currencies, commas, units) before performing arithmetic operations.
* **Case Sensitivity:** Ensured robust groupings by transforming all brand strings to uppercase during the initial extraction phase.
