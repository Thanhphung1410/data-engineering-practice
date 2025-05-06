import os
import zipfile
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    to_date, col, avg, count, month, desc, row_number, max as spark_max,
    year, current_date, expr
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


def average_trip_duration_per_day(df):
    return df.withColumn("date", to_date("start_time")) \
             .groupBy("date") \
             .agg(avg("tripduration").alias("avg_trip_duration"))


def trip_count_per_day(df):
    return df.withColumn("date", to_date("start_time")) \
             .groupBy("date") \
             .agg(count("*").alias("trip_count"))


def most_popular_start_station_per_month(df):
    df = df.withColumn("month", month("start_time"))
    window = Window.partitionBy("month").orderBy(desc("count"))
    return df.groupBy("month", "from_station_name") \
             .count() \
             .withColumn("rank", row_number().over(window)) \
             .filter(col("rank") == 1) \
             .drop("rank")


def top_3_popular_end_stations_last_14_days(df):
    df = df.withColumn("date", to_date("start_time"))
    max_date = df.select(spark_max("date")).first()[0]
    df = df.filter(col("date") >= expr(f"date_sub('{max_date}', 13)"))

    window = Window.partitionBy("date").orderBy(desc("count"))
    return df.groupBy("date", "to_station_name") \
             .count() \
             .withColumn("rank", row_number().over(window)) \
             .filter(col("rank") <= 3) \
             .drop("rank")


def average_trip_duration_by_gender(df):
    return df.groupBy("gender") \
             .agg(avg("tripduration").alias("avg_duration"))


def top_10_ages_by_trip_duration(df):
    df = df.withColumn("age", year(current_date()) - col("birthyear"))
    top_10_longest = df.orderBy(desc("tripduration")).select("age", "tripduration").limit(10)
    top_10_shortest = df.orderBy("tripduration").select("age", "tripduration").limit(10)
    return top_10_longest, top_10_shortest


def save_to_csv(df, filename):
    output_path = f"reports/{filename}"
    df.coalesce(1).write.csv(output_path, header=True, mode="overwrite")


def main():
    spark = SparkSession.builder.appName("Exercise6").getOrCreate()

    data_dir = "data"
    extract_zip_files(data_dir)

    df = load_data(spark, os.path.join(data_dir, "*.csv"))

    # Clean data (remove nulls in essential fields)
    df = df.filter(col("tripduration").isNotNull() & col("start_time").isNotNull())

    save_to_csv(average_trip_duration_per_day(df), "average_trip_duration_per_day")
    save_to_csv(trip_count_per_day(df), "trip_count_per_day")
    save_to_csv(most_popular_start_station_per_month(df), "most_popular_start_station_per_month")
    save_to_csv(top_3_popular_end_stations_last_14_days(df), "top_3_popular_end_stations_last_14_days")
    save_to_csv(average_trip_duration_by_gender(df), "avg_trip_duration_by_gender")

    top_longest, top_shortest = top_10_ages_by_trip_duration(df)
    save_to_csv(top_longest, "top_10_ages_longest_trips")
    save_to_csv(top_shortest, "top_10_ages_shortest_trips")

    spark.stop()


if __name__ == "__main__":
    main()
