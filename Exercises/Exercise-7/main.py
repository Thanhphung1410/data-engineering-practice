from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, unix_timestamp
import hashlib

def get_spark_session():
    """Khởi tạo SparkSession"""
    return SparkSession.builder.appName("HardDriveFailures").getOrCreate()

def add_source_file_column(df, source_file_name):
    """Thêm cột source_file vào DataFrame"""
    return df.withColumn("source_file", when(col("source_file").isNull(), source_file_name).otherwise(col("source_file")))

def extract_file_date(df):
    """Trích xuất date từ tên tệp và chuyển thành kiểu timestamp"""
    return df.withColumn("file_date", unix_timestamp(col("source_file").substr(1, 10), "yyyy-MM-dd").cast("timestamp"))

def add_brand_column(df):
    """Thêm cột brand từ model (tách tên thương hiệu từ model)"""
    return df.withColumn("brand", when(col("model").contains(" "), col("model").substr(1, col("model").find(" ") - 1)).otherwise("unknown"))

def add_storage_ranking(df):
    """Tạo cột storage_ranking từ capacity_bytes"""
    # Tạo DataFrame xếp hạng các model theo dung lượng capacity_bytes
    storage_ranking_df = df.select("model", "capacity_bytes").distinct().orderBy(col("capacity_bytes"), ascending=False).withColumn("storage_ranking", (col("capacity_bytes").rank().over(Window.orderBy(col("capacity_bytes").desc()))))
    return df.join(storage_ranking_df, on="model", how="left")

def add_primary_key(df):
    """Tạo cột primary_key bằng cách hash giá trị của mỗi bản ghi"""
    return df.withColumn("primary_key", hashlib.md5(col("serial_number").cast("string").encode("utf-8")).hexdigest())

def main():
    # Khởi tạo SparkSession
    spark = get_spark_session()
    
    # Đọc dữ liệu từ tệp CSV (ví dụ: hard-drive-2022-01-01-failures.csv)
    df = spark.read.option("header", "true").csv("datatraining/hard-drive-2022-01-01-failures.csv.zip")
    
    # Thêm cột source_file
    df = add_source_file_column(df, "hard-drive-2022-01-01-failures.csv.zip")
    
    # Trích xuất ngày từ tên tệp
    df = extract_file_date(df)
    
    # Thêm cột brand
    df = add_brand_column(df)
    
    # Xếp hạng dung lượng và thêm cột storage_ranking
    df = add_storage_ranking(df)
    
    # Thêm cột primary_key
    df = add_primary_key(df)
    
    # Hiển thị kết quả
    df.show()

if __name__ == "__main__":
    main()
