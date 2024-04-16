import sys
import unicodedata

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from logger import logger

from pyspark.sql.functions import (
    col,
    input_file_name,
    regexp_extract,
    lit,
    udf,
)
from pyspark.sql.types import StringType


def main():
    def remove_diacritics(text):
        nfkd_form = unicodedata.normalize("NFKD", text)
        ascii_string = nfkd_form.encode("ASCII", "ignore")
        return ascii_string.decode("utf-8")

    remove_diacritics_udf = udf(remove_diacritics, StringType())

    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "DATA_BUCKET_NAME",
            "PROCESSED_DATE",
            "TESTING_PREFIX",
            "TESTING_FILE_NAME",
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

    if args.get("TESTING_FILE_NAME") != "None":
        TESTING_FILE_NAME = args["TESTING_FILE_NAME"]
    else:
        TESTING_FILE_NAME = ""

    logger.info(f"DATA_BUCKET_NAME: {DATA_BUCKET_NAME}")
    logger.info(f"PROCESSED_DATE: {PROCESSED_DATE}")
    logger.info(f"TESTING_PREFIX: {TESTING_PREFIX}")
    logger.info(f"TESTING_FILE_NAME: {TESTING_FILE_NAME}")

    cities_paths = f"s3a://{DATA_BUCKET_NAME}/silver/cities/geographical/all_cities/{PROCESSED_DATE}/"

    logger.info("Starting all cities read...")

    all_cities = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(cities_paths)
    )

    logger.info("Finished all cities read")

    logger.info("Starting cities cost processing...")

    cities_cost_dir = f"s3a://{DATA_BUCKET_NAME}/bronze/cities/costs_by_id/{PROCESSED_DATE}/{TESTING_FILE_NAME}"
    file_name_pattern = ".*/(.*).csv"

    cities_all_costs = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(cities_cost_dir)
        .withColumn(
            "cost_id", regexp_extract(input_file_name(), file_name_pattern, 1)
        )
    )

    cities_ids = all_cities.select(
        col("name").alias("city_name_city_ids"),
        col("geonameid").alias("city_id"),
        "country_3_code",
    )

    cities_all_costs = cities_all_costs.withColumn(
        "city_name", remove_diacritics_udf(col("city_name"))
    )
    cities_ids = cities_ids.withColumn(
        "city_name_city_ids", remove_diacritics_udf(col("city_name_city_ids"))
    )

    costs_with_city_id = cities_all_costs.alias("all_costs").join(
        cities_ids.alias("cities_ids"),
        (col("cities_ids.country_3_code") == col("all_costs.country_code"))
        & (
            (col("city_name_city_ids").contains(col("city_name")))
            | (col("city_name").contains(col("city_name_city_ids")))
        ),
        "left",
    )

    match = (
        costs_with_city_id.where(~col("city_id").isNull())
        .drop("city_name", "country_code")
        .withColumnRenamed("city_name_city_ids", "city_name")
    )

    (
        match.withColumn("processed_date", lit(PROCESSED_DATE))
        .coalesce(1)
        .write.mode("overwrite")
        .options(header="True", delimiter=",")
        .partitionBy("country_3_code", "city_id", "processed_date")
        .csv(f"s3a://{DATA_BUCKET_NAME}{TESTING_PREFIX}/silver/cities/costs/")
    )

    no_match = costs_with_city_id.where(col("city_id").isNull())
    distinct_no_match = (
        no_match.select("city_name", "country_name", "country_code")
        .distinct()
        .count()
    )
    logger.info(f"Cities that didnt match {distinct_no_match}")

    costs_with_exact_match = cities_all_costs.alias("all_costs").join(
        cities_ids.alias("cities_ids"),
        (col("country_3_code") == col("country_code"))
        & (col("city_name_city_ids") == col("city_name")),
        "left",
    )
    no_exact_match = costs_with_exact_match.where(
        col("city_id").isNull()
    ).count()
    logger.info(f"Cities that didnt match exactly {no_exact_match}")

    logger.info("Finished cities cost processing")

    logger.info("Starting countries cost processing...")

    countries_costs_dir = f"s3a://{DATA_BUCKET_NAME}/bronze/countries/costs_by_id/{PROCESSED_DATE}/"
    file_name_pattern = ".*/(.*).csv"

    countries_all_costs = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(countries_costs_dir)
        .withColumn(
            "cost_id", regexp_extract(input_file_name(), file_name_pattern, 1)
        )
    )

    (
        countries_all_costs.withColumn("processed_date", lit(PROCESSED_DATE))
        .coalesce(1)
        .write.mode("overwrite")
        .options(header="True", delimiter=",")
        .partitionBy("country_code", "processed_date")
        .csv(
            f"s3a://{DATA_BUCKET_NAME}{TESTING_PREFIX}/silver/countries/costs/"
        )
    )

    logger.info("Finished countries cost processing")


if __name__ == "__main__":
    main()
