import sys

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from logger import logger

from pyspark.sql.functions import (
    col,
    lit,
    udf,
)
from pyspark.sql.types import FloatType
from math import radians, sin, cos, sqrt, atan2
from functools import reduce


def main():
    def haversine(lon1, lat1, lon2, lat2):
        R = 6371.0  # Radius of the Earth in kilometers
        dlon = radians(lon2) - radians(lon1)
        dlat = radians(lat2) - radians(lat1)
        a = (
            sin(dlat / 2) ** 2
            + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
        )
        c = 2 * atan2(sqrt(a), sqrt(1 - a))
        return R * c

    haversine_udf = udf(haversine, FloatType())

    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "DATA_BUCKET_NAME",
            "PROCESSED_DATE",
            "TESTING_PREFIX",
            "BIG_CITY_POP_THRESHOLD",
            "BIG_CITY_KM_THRESHOLD",
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
    BIG_CITY_POP_THRESHOLD = int(args["BIG_CITY_POP_THRESHOLD"])
    BIG_CITY_KM_THRESHOLD = float(args["BIG_CITY_KM_THRESHOLD"])

    if args.get("TESTING_PREFIX") != "None":
        TESTING_PREFIX = args["TESTING_PREFIX"]
    else:
        TESTING_PREFIX = ""

    logger.info(f"DATA_BUCKET_NAME: {DATA_BUCKET_NAME}")
    logger.info(f"PROCESSED_DATE: {PROCESSED_DATE}")
    logger.info(f"TESTING_PREFIX: {TESTING_PREFIX}")
    logger.info(f"BIG_CITY_POP_THRESHOLD: {BIG_CITY_POP_THRESHOLD}")
    logger.info(f"BIG_CITY_KM_THRESHOLD: {BIG_CITY_KM_THRESHOLD}")

    logger.info("Starting all cities read...")

    cities_paths = f"s3a://{DATA_BUCKET_NAME}/bronze/cities/geographical/cities_details/{PROCESSED_DATE}/"
    all_cities = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(cities_paths)
        .dropDuplicates(["geonameid"])
    )

    logger.info("Finished all cities read")

    logger.info("Starting all countries read...")

    countries_path = f"s3a://{DATA_BUCKET_NAME}/bronze/countries/geographical/countries_details/{PROCESSED_DATE}/countries_details.csv"
    all_countries = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(countries_path)
        .dropDuplicates(["geonameid"])
    )

    logger.info("Finished all countries read")

    logger.info("Starting all cities write...")

    all_cities = all_cities.join(
        all_countries.select("country_3_code", "continent_name"),
        on="country_3_code",
        how="left",
    )

    big_cities = all_cities.where(col("population") > BIG_CITY_POP_THRESHOLD)
    num_big_cities = big_cities.count()
    logger.info(f"Num of big cities: {num_big_cities}")

    cities_distance = all_cities
    for city in big_cities.collect():
        cities_distance = cities_distance.withColumn(
            "is_near_" + str(city["geonameid"]),
            haversine_udf(
                col("longitude"),
                col("latitude"),
                lit(city["longitude"]),
                lit(city["latitude"]),
            )
            < BIG_CITY_KM_THRESHOLD,
        )

    near_cols = [
        col_name for col_name in cities_distance.columns if "is_near_" in col_name
    ]
    condition = reduce(lambda x, y: x | y, (col(col_name) for col_name in near_cols))
    near_cities = cities_distance.where(condition)
    num_near_cities = near_cities.count()
    logger.info(f"Num of near cities: {num_near_cities}")

    (
        near_cities.select("geonameid", "name", "country_3_code", "population")
        .where(col("country_3_code") == "ESP")
        .show(100)
    )

    (
        near_cities.coalesce(1)
        .write.mode("overwrite")
        .options(header="True", delimiter=",")
        .csv(
            f"s3a://{DATA_BUCKET_NAME}{TESTING_PREFIX}/silver/cities/geographical/near_cities/{PROCESSED_DATE}/"
        )
    )

    cities_to_exclude = near_cities.where(
        col("population") < BIG_CITY_POP_THRESHOLD
    ).select("geonameid")
    final_cities = all_cities.join(cities_to_exclude, on="geonameid", how="left_anti")
    num_final_cities = final_cities.count()
    logger.info(f"Num of final cities: {num_final_cities}")

    (
        final_cities.coalesce(1)
        .write.mode("overwrite")
        .options(header="True", delimiter=",")
        .csv(
            f"s3a://{DATA_BUCKET_NAME}{TESTING_PREFIX}/silver/cities/geographical/all_cities/{PROCESSED_DATE}/"
        )
    )

    logger.info("Finished all cities write")


if __name__ == "__main__":
    main()
