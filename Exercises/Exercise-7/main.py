import os
import zipfile
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, when, hash, split, monotonically_increasing_id
)
from pyspark.sql.window import Window

def extract_zip_files(data_dir):
    for filename in os.listdir(data_dir):
        if filename.endswith(".zip"):
            zip_path = os.path.join(data_dir, filename)
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(data_dir)


def load_data(spark, data_path):
    return spark.read.csv(data_path, header=True, inferSchema=True)


def add_source_file_column(df, data_path):
    # Thêm tên tệp vào cột source_file
    source_file = os.path.basename(data_path)
    return df.withColumn("source_file", when(col("source_file").isNull(), source_file).otherwise(col("source_file")))


def extract_date_from_source_file(df):
    # Tạo cột file_date từ chuỗi trong cột source_file
    return df.withColumn("file_date", to_date(split(col("source_file"), "-")[0]))


def add_brand_column(df):
    # Tạo cột brand từ cột model
    return df.withColumn("brand", when(col("model").contains(" "), split(col("model"), " ")[0]).otherwise("unknown"))


def add_storage_ranking(df):
    # Xếp hạng dung lượng của các model theo thứ tự từ lớn nhất đến nhỏ nhất
    window_spec = Window.orderBy(col("capacity_bytes").desc())
    return df.withColumn("storage_ranking", row_number().over(window_spec))


def add_primary_key(df):
    # Tạo primary_key từ hash của các cột
    return df.withColumn("primary_key", hash(*df.columns))


def save_to_csv(df, filename):
    output_path = f"reports/{filename}"
    df.coalesce(1).write.csv(output_path, header=True, mode="overwrite")


def main():
    spark = SparkSession.builder.appName("Exercise7").getOrCreate()

    data_dir = "data"
    extract_zip_files(data_dir)

    # Load dữ liệu từ các tệp CSV đã giải nén
    df = load_data(spark, os.path.join(data_dir, "*.csv"))

    # Thêm cột source_file
    df = add_source_file_column(df, os.path.join(data_dir, "hard-drive-2022-01-01-failures.csv.zip"))
    
    # Thêm cột file_date
    df = extract_date_from_source_file(df)
    
    # Thêm cột brand
    df = add_brand_column(df)
    
    # Thêm cột storage_ranking
    df = add_storage_ranking(df)
    
    # Thêm cột primary_key
    df = add_primary_key(df)

    # Lưu các kết quả vào các tệp CSV
    save_to_csv(df, "final_report_with_new_columns")

    spark.stop()


if __name__ == "__main__":
    main()
