import sys

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from logger import logger
from pyspark.sql.functions import (
    col,
    count,
    avg,
    regexp_extract,
    lit,
    from_json,
    explode,
    round,
    when,
    first,
    row_number,
    collect_list,
    expr,
    concat_ws,
)
from pyspark.sql.window import Window
from pyspark.sql.types import MapType, StringType


def main():
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "DATA_BUCKET_NAME",
            "PROCESSED_DATE",
            "TESTING_PREFIX",
        ],
    )

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    spark.conf.set(
        "spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false"
    )

    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    DATA_BUCKET_NAME = args["DATA_BUCKET_NAME"]
    PROCESSED_DATE = args["PROCESSED_DATE"]

    if args.get("TESTING_PREFIX") != "None":
        TESTING_PREFIX = args["TESTING_PREFIX"]
    else:
        TESTING_PREFIX = ""

    logger.info(f"DATA_BUCKET_NAME: {DATA_BUCKET_NAME}")
    logger.info(f"PROCESSED_DATE: {PROCESSED_DATE}")
    logger.info(f"TESTING_PREFIX: {TESTING_PREFIX}")

    logger.info("Starting city tags processing...")

    weather_path = f"s3a://{DATA_BUCKET_NAME}/silver/cities/weather/"

    weather = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(weather_path)
    )

    tags = (
        weather.groupBy("city_id", "timezone")
        .agg(
            round(avg(col("avg_cloud_cover_percent"))).alias("avg_cloud_cover_percent"),
            round(avg(col("avg_humidity_2m_percent"))).alias("avg_humidity_2m_percent"),
            round(avg(col("avg_daily_temp_celsius")), 2).alias(
                "avg_daily_temp_celsius"
            ),
            round(avg(col("avg_daily_precipitation_hours")), 2).alias(
                "avg_daily_precipitation_hours"
            ),
            round(avg(col("avg_european_aqi"))).alias("avg_european_aqi"),
            round(avg(col("avg_daylight_hours"))).alias("avg_daylight_hours"),
        )
        .withColumn(
            "cloud_coverage_tag",
            when(col("avg_cloud_cover_percent") < 10, "clear")
            .when(col("avg_cloud_cover_percent") < 25, "mostly clear")
            .when(col("avg_cloud_cover_percent") < 50, "partly cloudy")
            .when(col("avg_cloud_cover_percent") < 75, "mostly cloudy")
            .otherwise("very cloudy"),
        )
        .withColumn(
            "humidity_tag",
            when(col("avg_humidity_2m_percent") < 40, "very dry")
            .when(col("avg_humidity_2m_percent") < 55, "dry")
            .when(col("avg_humidity_2m_percent") < 70, "moderate")
            .when(col("avg_humidity_2m_percent") < 85, "humid")
            .otherwise("very humid"),
        )
        .withColumn(
            "temp_tag",
            when(col("avg_daily_temp_celsius") < 0, "very cold")
            .when(col("avg_daily_temp_celsius") < 10, "cold")
            .when(col("avg_daily_temp_celsius") < 20, "mild")
            .when(col("avg_daily_temp_celsius") < 30, "hot")
            .otherwise("very hot"),
        )
        .withColumn(
            "precipitation_tag",
            when(col("avg_daily_precipitation_hours") < 1, "dry")
            .when(col("avg_daily_precipitation_hours") < 2, "slightly dry")
            .when(col("avg_daily_precipitation_hours") < 3, "moderate")
            .when(col("avg_daily_precipitation_hours") < 4, "moderately wet")
            .when(col("avg_daily_precipitation_hours") < 5, "wet")
            .otherwise("very wet"),
        )
        .withColumn(
            "air_quality_tag",
            when(col("avg_european_aqi") < 20, "good")
            .when(col("avg_european_aqi") < 40, "fair")
            .when(col("avg_european_aqi") < 60, "moderate")
            .when(col("avg_european_aqi") < 80, "poor")
            .when(col("avg_european_aqi") < 100, "very poor")
            .otherwise("extremely poor"),
        )
        .withColumn(
            "daylight_hours_tag",
            when(col("avg_daylight_hours") < 4, "very low")
            .when(col("avg_daylight_hours") < 8, "low")
            .when(col("avg_daylight_hours") < 12, "moderate")
            .when(col("avg_daylight_hours") < 16, "high")
            .otherwise("very high"),
        )
        .select(
            "city_id",
            "avg_cloud_cover_percent",
            "avg_humidity_2m_percent",
            "avg_daily_temp_celsius",
            "avg_daily_precipitation_hours",
            "avg_european_aqi",
            "avg_daylight_hours",
            "cloud_coverage_tag",
            "humidity_tag",
            "temp_tag",
            "precipitation_tag",
            "air_quality_tag",
            "daylight_hours_tag",
        )
    )

    cities_paths = f"s3a://{DATA_BUCKET_NAME}/silver/cities/geographical/all_cities/{PROCESSED_DATE}/"
    all_cities = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(cities_paths)
        .select(
            col("geonameid").alias("city_id"),
            "name",
            "population",
            "continent_name",
            "country_3_code",
        )
    )

    tags_city_info = (
        tags.join(all_cities, on="city_id", how="inner")
        .withColumnRenamed("continent_name", "continent_tag")
        .withColumn(
            "city_size_tag",
            when(col("population") < 200000, "small")
            .when(col("population") < 500000, "moderate")
            .when(col("population") < 1000000, "big")
            .when(col("population") < 5000000, "very big")
            .otherwise("giant"),
        )
    )

    (
        tags_city_info.select(
            "city_id",
            "name",
            "population",
            "avg_cloud_cover_percent",
            "cloud_coverage_tag",
            "avg_daily_precipitation_hours",
            "precipitation_tag",
            "avg_european_aqi",
            "air_quality_tag",
            "avg_humidity_2m_percent",
            "humidity_tag",
            "avg_daily_temp_celsius",
            "temp_tag",
            "avg_daylight_hours",
            "daylight_hours_tag",
            "city_size_tag",
            "continent_tag",
        ).where(col("country_3_code") == "ESP")
    ).show(10)

    (
        tags_city_info.groupBy("cloud_coverage_tag")
        .agg(count("*").alias("num_cities"))
        .orderBy("num_cities")
    ).show(10)

    (
        tags_city_info.groupBy("humidity_tag")
        .agg(count("*").alias("num_cities"))
        .orderBy("num_cities")
    ).show(10)

    (
        tags_city_info.groupBy("temp_tag")
        .agg(count("*").alias("num_cities"))
        .orderBy("num_cities")
    ).show(10)

    (
        tags_city_info.groupBy("precipitation_tag")
        .agg(count("*").alias("num_cities"))
        .orderBy("num_cities")
    ).show(10)

    tags_city_info.agg(avg("avg_daily_precipitation_hours")).show(10)

    (
        tags_city_info.groupBy("air_quality_tag")
        .agg(count("*").alias("num_cities"))
        .orderBy("num_cities")
    ).show(10)

    (
        tags_city_info.withColumn("processed_date", lit(PROCESSED_DATE))
        .coalesce(1)
        .write.mode("overwrite")
        .options(header="True", delimiter=",")
        .partitionBy("processed_date")
        .csv(f"s3a://{DATA_BUCKET_NAME}/gold/cities/tags/")
    )

    logger.info("Finished city tags processing")
    logger.info("Starting country tags processing...")

    demographics_path = f"s3a://{DATA_BUCKET_NAME}/bronze/countries/geographical/countries_details/11-10-2023/"

    demographics = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(demographics_path)
        .withColumn(
            "area_size",
            regexp_extract(col("area_size"), r"(\d+\.\d+)", 1).cast("double"),
        )
    )

    demographics_tags = (
        demographics.select(
            "country_3_code",
            "languages",
            "area_size",
            explode(
                from_json(col("languages"), MapType(StringType(), StringType()))
            ).alias("lang_key", "language_name"),
        )
        .drop("lang_key")
        .groupBy("country_3_code", "area_size")
        .agg(collect_list("language_name").alias("languages"))
        .withColumn(
            "languages_tag",
            concat_ws(
                ",",
                expr("TRANSFORM(languages, x -> regexp_replace(x, ' language', ''))"),
            ),
        )
        .withColumn(
            "area_size",
            regexp_extract(col("area_size"), r"(\d+\.\d+)", 1).cast("double"),
        )
        .withColumn(
            "country_size_tag",
            when(col("area_size").isNull(), None)
            .when(col("area_size") < 1000.0, "very small")
            .when(col("area_size") < 100000.0, "small")
            .when(col("area_size") < 1000000.0, "moderate")
            .when(col("area_size") < 5000000.0, "big")
            .otherwise("very big"),
        )
    )

    demographics_tags.show(10)

    taxes_path = f"s3a://{DATA_BUCKET_NAME}/silver/countries/taxes/"

    taxes = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(taxes_path)
        .withColumnRenamed("country_code", "country_3_code")
    )

    taxes_1_row = taxes.groupBy("country_3_code").pivot("tax_type").agg(first("value"))

    taxes_1_row.show(10)

    taxes_tags = (
        taxes_1_row.withColumn(
            "income_tax_tag",
            when(col("income_tax_rate").isNull(), None)
            .when(col("income_tax_rate") < 10.0, "very low")
            .when(col("income_tax_rate") < 20.0, "low")
            .when(col("income_tax_rate") < 30.0, "moderate")
            .when(col("income_tax_rate") < 40.0, "high")
            .otherwise("very high"),
        )
        .withColumn(
            "sales_tax_tag",
            when(col("sales_tax_rate").isNull(), None)
            .when(col("sales_tax_rate") < 5.0, "very low")
            .when(col("sales_tax_rate") < 10.0, "low")
            .when(col("sales_tax_rate") < 15.0, "moderate")
            .when(col("sales_tax_rate") < 20.0, "high")
            .otherwise("very high"),
        )
        .withColumn(
            "social_security_tag",
            when(col("social_security_rate").isNull(), None)
            .when(col("social_security_rate") < 5.0, "very low")
            .when(col("social_security_rate") < 10.0, "low")
            .when(col("social_security_rate") < 15.0, "moderate")
            .when(col("social_security_rate") < 20.0, "high")
            .otherwise("very high"),
        )
    )

    taxes_tags.show(10)

    taxes_tags.where(col("country_3_code") == "ESP").show()

    taxes_tags.where(col("social_security_rate") > 15.0).show()

    indicators_path = f"s3a://{DATA_BUCKET_NAME}/silver/countries/indicators/"

    indicators = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(indicators_path)
        .withColumnRenamed("country_code", "country_3_code")
    )

    indicators.show(10)

    windowSpec = Window.partitionBy("country_3_code", "indicator_type").orderBy(
        col("year").desc()
    )

    indicators_latest = (
        indicators.where(col("indicator_type").isin(["inflation", "unemployment_rate"]))
        .where(col("year") <= 2023)
        .withColumn("row_num", row_number().over(windowSpec))
        .where(col("row_num") == 1)
        .drop("row_num")
    )

    indicators_latest.select("indicator_type", "unit").distinct().show()

    indicators_tags = (
        indicators_latest.groupBy("country_3_code")
        .pivot("indicator_type")
        .agg(first("value"))
        .withColumn(
            "inflation_tag",
            when(col("inflation").isNull(), None)
            .when(col("inflation") < 0.0, "deflation")
            .when(col("inflation") < 2.0, "stable")
            .when(col("inflation") < 5.0, "high")
            .when(col("inflation") < 10.0, "very high")
            .otherwise("hyperinflation"),
        )
        .withColumn(
            "unemployment_tag",
            when(col("unemployment_rate").isNull(), None)
            .when(col("unemployment_rate") < 3.0, "full employment")
            .when(col("unemployment_rate") < 6.0, "low unemployment")
            .when(col("unemployment_rate") < 8.0, "moderate unemployment")
            .when(col("unemployment_rate") < 10.0, "high")
            .otherwise("very high"),
        )
    )

    indicators_tags.show(10)

    indicators_tags.where(col("country_3_code") == "ESP").show()

    indicators_cols = indicators_tags.columns
    taxes_cols = taxes_tags.columns
    demographics_cols = demographics_tags.columns
    keep_cols = ["country_3_code", "tag"]

    keep_indicators_cols = [
        col
        for col in indicators_cols
        if any(substring in col for substring in keep_cols)
    ]
    keep_taxes_cols = [
        col for col in taxes_cols if any(substring in col for substring in keep_cols)
    ]
    keep_demographics_cols = [
        col
        for col in demographics_cols
        if any(substring in col for substring in keep_cols)
    ]

    country_tags = (
        indicators_tags.select(keep_indicators_cols)
        .join(taxes_tags.select(keep_taxes_cols), how="fullouter", on="country_3_code")
        .join(
            demographics_tags.select(keep_demographics_cols),
            how="fullouter",
            on="country_3_code",
        )
    )

    country_tags.show(10)

    (
        country_tags.withColumn("processed_date", lit(PROCESSED_DATE))
        .coalesce(1)
        .write.mode("overwrite")
        .options(header="True", delimiter=",")
        .partitionBy("country_3_code", "processed_date")
        .csv(f"s3a://{DATA_BUCKET_NAME}/gold/countries/tags/")
    )

    logger.info("Finished country tags processing")


if __name__ == "__main__":
    main()
