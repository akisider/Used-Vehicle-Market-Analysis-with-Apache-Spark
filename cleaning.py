"""Shared cleaning logic for the used vehicle pipeline.

Both the batch and the streaming job import from here, so the same
rules are applied to every record no matter how it arrives.
"""

from pyspark.sql.functions import col, count, lit, regexp_replace, trim, upper, when
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# Conversion factors used in the analysis.
# USD_TO_EUR is a fixed rate chosen for the assignment, not a live exchange rate.
USD_TO_EUR = 0.9
MILES_TO_KM = 1.60934

# Raw input schema. Price and mileage arrive as text ("$10,300", "51,000 mi."),
# so they are read as strings and only cast to numbers after cleaning.
# Reading them directly as numbers makes Spark silently turn them into nulls.
RAW_SCHEMA = StructType([
    StructField("brand", StringType()),
    StructField("model", StringType()),
    StructField("model_year", IntegerType()),
    StructField("milage", StringType()),
    StructField("fuel_type", StringType()),
    StructField("engine", StringType()),
    StructField("transmission", StringType()),
    StructField("ext_col", StringType()),
    StructField("int_col", StringType()),
    StructField("accident", StringType()),
    StructField("clean_title", StringType()),
    StructField("price", StringType()),
])

# Two-word brands that the source data splits across brand and model,
# e.g. brand "Land", model "Rover Range Rover Sport".
SPLIT_BRANDS = {"LAND": "Rover", "ASTON": "Martin", "ALFA": "Romeo"}

# Placeholder text in fuel_type that really means "missing".
MISSING_FUEL_VALUES = ["–", "not supported"]


def fix_split_brands(df):
    """Join two-word brands back together and remove the extra word from the model."""
    for first, second in SPLIT_BRANDS.items():
        is_split = (upper(trim(col("brand"))) == first) & col("model").startswith(second + " ")
        df = (
            df.withColumn("_is_split", is_split)
              .withColumn("brand", when(col("_is_split"), lit(first + " " + second.upper()))
                                   .otherwise(col("brand")))
              .withColumn("model", when(col("_is_split"), regexp_replace(col("model"), "^" + second + " ", ""))
                                   .otherwise(col("model")))
              .drop("_is_split")
        )
    return df


def clean_vehicles(df):
    """Standardise text fields and turn price and mileage into numbers."""
    df = fix_split_brands(df)
    return (
        df.withColumn("brand", upper(trim(col("brand"))))
          .withColumn("model", regexp_replace(col("model"), " ", "_"))
          .withColumn("engine", regexp_replace(col("engine"), " ", "_"))
          # Keep only digits and the decimal point, then cast: "$10,300" -> 10300.0
          .withColumn("milage", regexp_replace(col("milage"), "[^0-9.]", "").cast("double"))
          .withColumn("price", regexp_replace(col("price"), "[^0-9.]", "").cast("double"))
          .withColumn("fuel_type", when(col("fuel_type").isin(MISSING_FUEL_VALUES), None)
                                   .otherwise(col("fuel_type")))
    )


def to_metric(df):
    """Add EUR and km columns, keeping the original USD and miles values."""
    return (
        df.withColumn("price_eur", col("price") * USD_TO_EUR)
          .withColumn("milage_km", col("milage") * MILES_TO_KM)
    )


def count_nulls(df, columns):
    """Return {column: number of nulls} for the given columns."""
    row = df.select([count(when(col(c).isNull(), c)).alias(c) for c in columns]).first()
    return row.asDict()
