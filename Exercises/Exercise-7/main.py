from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    input_file_name, regexp_extract, to_date, split, when, col,
    dense_rank, sha2, concat_ws
)
from pyspark.sql.window import Window

def process_data(spark, input_path):
    # 1. Đọc file zip (CSV nén)
    df = spark.read.option("header", "true").csv(f"zip://{input_path}")

    # 2. Thêm cột source_file
    df = df.withColumn("source_file", input_file_name())

    # 3. Trích xuất ngày từ tên file -> file_date
    df = df.withColumn(
        "file_date",
        to_date(regexp_extract("source_file", r"(\d{4}-\d{2}-\d{2})", 1), "yyyy-MM-dd")
    )

    # 4. Thêm cột brand từ model
    df = df.withColumn(
        "brand",
        when(col("model").contains(" "), split(col("model"), " ")[0]).otherwise("unknown")
    )

    # 5. Tính storage_ranking theo capacity_bytes
    window_spec = Window.orderBy(col("capacity_bytes").cast("bigint").desc())
    df = df.withColumn(
        "storage_ranking",
        dense_rank().over(window_spec)
    )

    # 6. Tạo primary_key bằng sha2 (hash 256)
    df = df.withColumn(
        "primary_key",
        sha2(concat_ws("||", *df.columns), 256)
    )

    return df

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Exercise-7").getOrCreate()

    input_file = "data/hard-drive-2022-01-01-failures.csv.zip"
    result_df = process_data(spark, input_file)

    result_df.show(5, truncate=False)
    spark.stop()

