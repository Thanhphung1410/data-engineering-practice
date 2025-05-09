import os
import zipfile
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract, col, split, when, monotonically_increasing_id, row_number
from pyspark.sql.window import Window
import hashlib

def extract_zip(zip_path, extract_to="data"):
    if not os.path.exists(extract_to):
        os.makedirs(extract_to)
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)

def compute_primary_key(*cols):
    concat_str = "_".join(str(c) for c in cols)
    return hashlib.sha256(concat_str.encode()).hexdigest()

def main():
    # Giải nén file zip trước
    zip_path = "data/hard-drive-2022-01-01-failures.csv.zip"
    extract_zip(zip_path, "data")

    spark = SparkSession.builder \
        .appName("Exercise 7") \
        .getOrCreate()

    df = spark.read.option("header", "true").csv("data/*.csv")

    # Add source_file column
    df = df.withColumn("source_file", input_file_name())

    # Extract file_date from filename (dạng yyyy-MM-dd)
    df = df.withColumn("file_date", regexp_extract("source_file", r"(\d{4}-\d{2}-\d{2})", 1).cast("date"))

    # Add brand column
    df = df.withColumn(
        "brand",
        when(col("model").contains(" "), split(col("model"), " ")[0]).otherwise("unknown")
    )

    # Create storage_ranking
    window_spec = Window.orderBy(col("capacity_bytes").cast("long").desc())
    df = df.withColumn("storage_ranking", row_number().over(window_spec))

    # Create primary_key (hash of model + serial_number + date)
    df = df.withColumn(
        "primary_key",
        col("serial_number") + "_" + col("model") + "_" + col("date")
    )
    # Convert to SHA256 hash (optional)
    df = df.withColumn("primary_key", 
        col("primary_key")
    )

    # Show sample
    df.select("serial_number", "model", "brand", "capacity_bytes", "storage_ranking", "primary_key", "file_date", "source_file").show(10, truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
