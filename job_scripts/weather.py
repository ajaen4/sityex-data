import sys

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from logger import logger

from pyspark.sql.functions import (
    col,
    avg,
    input_file_name,
    regexp_extract,
    lit,
    to_date,
    year,
    month,
    hour,
    minute,
    second,
    round,
    floor,
    broadcast,
)


def main():
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "DATA_BUCKET_NAME",
            "PROCESSED_DATE",
            "TESTING_PREFIX",
            "TESTING_CITY_ID",
        ],
    )

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    spark.conf.set(
        "spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs",
        "false",
    )

    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    DATA_BUCKET_NAME = args["DATA_BUCKET_NAME"]
    PROCESSED_DATE = args["PROCESSED_DATE"]

    if args.get("TESTING_PREFIX") != "None":
        TESTING_PREFIX = args["TESTING_PREFIX"]
    else:
        TESTING_PREFIX = ""

    if args.get("TESTING_CITY_ID") != "None":
        TESTING_CITY_ID = args["TESTING_CITY_ID"]
    else:
        TESTING_CITY_ID = ""

    if TESTING_CITY_ID:
        DIRECTORY_PATTERN = f"s3a://{DATA_BUCKET_NAME}/bronze/cities/weather/{TESTING_CITY_ID}/{PROCESSED_DATE}"
    else:
        DIRECTORY_PATTERN = f"s3a://{DATA_BUCKET_NAME}/bronze/cities/weather/*/{PROCESSED_DATE}"

    logger.info(f"DATA_BUCKET_NAME: {DATA_BUCKET_NAME}")
    logger.info(f"PROCESSED_DATE: {PROCESSED_DATE}")
    logger.info(f"TESTING_PREFIX: {TESTING_PREFIX}")
    logger.info(f"TESTING_CITY_ID: {TESTING_CITY_ID}")
    logger.info(f"DIRECTORY_PATTERN: {DIRECTORY_PATTERN}")

    logger.info("Starting weather processing...")

    logger.info("Reading all cities...")
    cities_paths = f"s3a://{DATA_BUCKET_NAME}/silver/cities/geographical/all_cities/{PROCESSED_DATE}/"
    all_cities = broadcast(
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(cities_paths)
    )
    if TESTING_CITY_ID != "":
        all_cities = all_cities.where(col("geonameid") == TESTING_CITY_ID)

    logger.info("Finished reading all cities")

    FILE_NAME_PATTERN = f".*/weather/(.*)/{PROCESSED_DATE}/.*csv"

    daily_weather_pattern = f"{DIRECTORY_PATTERN}/daily_weather.csv"

    daily_weather = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(daily_weather_pattern)
        .withColumn(
            "city_id", regexp_extract(input_file_name(), FILE_NAME_PATTERN, 1)
        )
    )

    daily_weather = all_cities.select(col("geonameid").alias("city_id")).join(
        daily_weather, how="left", on="city_id"
    )

    daily_num_cities = daily_weather.select("city_id").distinct().count()
    logger.info(f"Num cities in daily weather {daily_num_cities}")
    daily_weather.show(12)

    daily_monthly_metrics = (
        daily_weather.withColumn("date", to_date("date"))
        .withColumn("year", year("date"))
        .withColumn("month", month("date"))
        .withColumn(
            "sunrise_seconds",
            hour("sunrise") * 3600
            + minute("sunrise") * 60
            + second("sunrise"),
        )
        .withColumn(
            "sunset_seconds",
            hour("sunset") * 3600 + minute("sunset") * 60 + second("sunset"),
        )
        .withColumn(
            "daylight_hours",
            (col("sunset_seconds") - col("sunrise_seconds")) / 3600,
        )
        .groupBy("year", "month", "city_id", "timezone")
        .agg(
            avg("sunrise_seconds").alias("avg_sunrise_seconds"),
            avg("sunset_seconds").alias("avg_sunset_seconds"),
            round(avg("mean_temperature_celsius"), 2).alias(
                "avg_daily_temp_celsius"
            ),
            round(avg("precipitation_hours"), 2).alias(
                "avg_daily_precipitation_hours"
            ),
            round(avg("daylight_hours")).alias("avg_daylight_hours"),
        )
        .withColumn(
            "avg_sunrise_hour", floor(col("avg_sunrise_seconds") / 3600)
        )
        .withColumn(
            "avg_sunrise_minute",
            floor((col("avg_sunrise_seconds") % 3600) / 60),
        )
        .withColumn(
            "avg_sunrise_second", floor(col("avg_sunrise_seconds") % 60)
        )
        .withColumn("avg_sunset_hour", floor(col("avg_sunset_seconds") / 3600))
        .withColumn(
            "avg_sunset_minute", floor((col("avg_sunset_seconds") % 3600) / 60)
        )
        .withColumn("avg_sunset_second", floor(col("avg_sunset_seconds") % 60))
        .select(
            "year",
            "month",
            "timezone",
            "city_id",
            "avg_sunrise_hour",
            "avg_sunrise_minute",
            "avg_sunrise_second",
            "avg_sunset_hour",
            "avg_sunset_minute",
            "avg_sunset_second",
            "avg_daily_temp_celsius",
            "avg_daily_precipitation_hours",
            "avg_daylight_hours",
        )
    )

    daily_madrid_statistics = (
        daily_monthly_metrics.where(col("city_id") == "3117735")
        .select(
            "month",
            "avg_sunrise_hour",
            "avg_sunrise_minute",
            "avg_sunrise_second",
            "avg_sunset_hour",
            "avg_sunset_minute",
            "avg_sunset_second",
            "avg_daily_temp_celsius",
            "avg_daily_precipitation_hours",
            "avg_daylight_hours",
        )
        .orderBy("month")
    )
    daily_madrid_statistics.show(12)

    hourly_weather_pattern = f"{DIRECTORY_PATTERN}/hourly_weather.csv"

    hourly_weather = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(hourly_weather_pattern)
        .withColumn(
            "city_id", regexp_extract(input_file_name(), FILE_NAME_PATTERN, 1)
        )
    )
    hourly_weather = all_cities.select(col("geonameid").alias("city_id")).join(
        hourly_weather, how="left", on="city_id"
    )

    hourly_num_cities = hourly_weather.select("city_id").distinct().count()
    logger.info(f"Num cities in hourly weather {hourly_num_cities}")
    hourly_weather.show(12)

    hourly_monthly_metrics = (
        hourly_weather.withColumn("date", to_date("timestamp"))
        .withColumn("year", year("timestamp"))
        .withColumn("month", month("timestamp"))
        .groupBy("year", "month", "city_id", "timezone")
        .agg(
            avg("cloud_cover_percent")
            .cast("int")
            .alias("avg_cloud_cover_percent"),
            avg("relativehumidity_2m_percent")
            .cast("int")
            .alias("avg_humidity_2m_percent"),
        )
        .select(
            "city_id",
            "year",
            "month",
            "timezone",
            "avg_cloud_cover_percent",
            "avg_humidity_2m_percent",
        )
    )

    hourly_madrid_statistics = (
        hourly_monthly_metrics.where(col("city_id") == "3117735")
        .select(
            "month",
            "avg_cloud_cover_percent",
            "avg_humidity_2m_percent",
        )
        .orderBy("month")
    )
    hourly_madrid_statistics.show()

    air_qual_pattern = f"{DIRECTORY_PATTERN}/air_quality.csv"

    air_quality = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(air_qual_pattern)
        .withColumn(
            "city_id", regexp_extract(input_file_name(), FILE_NAME_PATTERN, 1)
        )
    )

    air_quality_num_cities = air_quality.select("city_id").distinct().count()
    logger.info(f"Num cities in air quality {air_quality_num_cities}")
    air_quality.show(12)

    air_qual_metrics = (
        air_quality.withColumn("date", to_date("timestamp"))
        .withColumn("year", year("timestamp"))
        .withColumn("month", month("timestamp"))
        .groupBy("year", "month", "city_id", "timezone")
        .agg(
            avg("european_aqi").cast("int").alias("avg_european_aqi"),
        )
        .select(
            "city_id",
            "year",
            "month",
            "timezone",
            "avg_european_aqi",
        )
    )

    air_qual_madrid_statistics = (
        air_qual_metrics.where(col("city_id") == "3117735")
        .select(
            "month",
            "avg_european_aqi",
        )
        .orderBy("month")
    )
    air_qual_madrid_statistics.show()

    all_weather = (
        hourly_monthly_metrics.drop("year")
        .join(
            daily_monthly_metrics.drop("year"),
            ["city_id", "month", "timezone"],
            "inner",
        )
        .join(
            air_qual_metrics.drop("year"),
            ["city_id", "month", "timezone"],
            "inner",
        )
    )
    num_cities_all = all_weather.select("city_id").distinct().count()
    logger.info(f"Num cities in all weather {num_cities_all}")

    madrid_weather = all_weather.where(col("city_id") == "3117735")
    madrid_weather.show()

    (
        all_weather.withColumn("processed_date", lit(PROCESSED_DATE))
        .coalesce(1)
        .write.mode("overwrite")
        .options(header="True", delimiter=",")
        .partitionBy("city_id", "processed_date")
        .csv(
            f"s3a://{DATA_BUCKET_NAME}{TESTING_PREFIX}/silver/cities/weather/"
        )
    )

    logger.info("Finished weather write")


if __name__ == "__main__":
    main()
