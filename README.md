# Used Vehicle Market Analysis with Apache Spark

## Project Overview
This project builds a data pipeline with **Apache Spark (PySpark)** to clean and analyze a dataset of about 4,000 used car listings. It includes both a Batch Processing and a Structured Streaming implementation that share the same cleaning logic.

## Tech Stack
* **Language:** Python
* **Data Processing:** Apache Spark (PySpark), Spark SQL
* **Data Format:** CSV, DataFrames

## Project Structure
* `cleaning.py` – shared schema and cleaning functions used by both jobs
* `batch_processing.py` – batch pipeline, data quality checks and statistics
* `stream_processing.py` – Structured Streaming pipeline on an input folder
* `used_cars.csv` – source dataset

## Key Features & Pipeline Steps

### 1. Shared cleaning (`cleaning.py`)
* **Explicit raw schema:** price and mileage are read as text and only cast after cleaning.
* **Data Cleansing:** Standardized string formats (uppercase brands, spaces replaced with underscores in models/engines).
* **Brand repair:** two-word brands split across columns in the source data (e.g. `Land` + `Rover ...`) are joined back together (`LAND ROVER`, `ASTON MARTIN`, `ALFA ROMEO`).
* **Hidden missing values:** placeholder text in `fuel_type` (`–`, `not supported`) is converted to null.
* **Data Type Casting:** dirty string values (e.g. `$51,000`, `51,000 mi.`) are converted into `DoubleType` with Regex (`regexp_replace`).
* **Unit conversion:** adds `price_eur` and `milage_km` columns while keeping the original USD and miles values.

### 2. Batch Processing (`batch_processing.py`)
* **Data quality check:** compares null counts before and after cleaning and warns when values could not be converted to numbers.
* **Feature Engineering:** "Value Index" (mileage × price), as defined in the assignment.
* **Aggregation:** market statistics per brand and model (average price, vehicle count, best value index).

### 3. Structured Streaming (`stream_processing.py`)
* Monitors an input directory for new CSV files.
* Applies exactly the same cleaning and transformation logic (imported from `cleaning.py`) to incoming micro-batches.
* Maintains running averages and vehicle counts per brand and model.

## How to Run
```bash
spark-submit batch_processing.py
spark-submit stream_processing.py   # then copy CSV files into stream_input/
```

## Challenges Solved
* **Silent type casting failures:** casting text like `51,000 mi.` to a number returns null instead of failing, so non-numeric characters (currencies, commas, units) are stripped first and null counts are checked before and after.
* **Schema mismatch in streaming:** the stream originally reused the schema of the *cleaned* batch DataFrame, so raw price and mileage text was read as numbers and became null. It now uses the raw schema from `cleaning.py`.
* **Duplicated logic:** cleaning rules were moved into one shared module so batch and streaming results stay consistent.
* **Case Sensitivity:** brands are uppercased so that groupings are consistent.

## Known Limitations
* The value index (mileage × price) favours almost-new cars rather than good-value cars. Comparing each price with the expected price for the car's age and mileage would be a better measure.
* `USD_TO_EUR` is a fixed rate, not a live exchange rate.
