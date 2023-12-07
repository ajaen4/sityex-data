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
    input_file_name,
    regexp_extract,
    lit,
    from_json,
    translate,
    explode,
    concat,
    split,
    lower,
)
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

    logger.info("Starting indicators processing...")
    logger.info("Starting inflation processing...")

    map_schema = MapType(StringType(), StringType())

    file_name = f"s3a://{DATA_BUCKET_NAME}/bronze/countries/indicators/{PROCESSED_DATE}/inflation.csv"
    file_name_pattern = ".*/(.*).csv"

    inflation = (
        spark.read.format("csv")
        .option("header", "true")
        .load(file_name)
        .withColumn(
            "indicator_type", regexp_extract(input_file_name(), file_name_pattern, 1)
        )
        .withColumn("json_col", translate(col("values_per_year"), "'", '"'))
        .withColumn("map", from_json(col("json_col"), map_schema))
        .select(
            "country_code",
            explode(col("map")),
            lit(None).alias("unit"),
            "indicator_type",
        )
        .withColumnRenamed("key", "year")
        .withColumnRenamed("value", "value")
    )
    inflation.show(10)
    logger.info("Finished inflation processing")

    logger.info("Starting unemployment rate processing...")

    file_name = f"s3a://{DATA_BUCKET_NAME}/bronze/countries/indicators/{PROCESSED_DATE}/unemployment_rate.csv"
    file_name_pattern = ".*/(.*).csv"

    unemployment_rate = (
        spark.read.format("csv")
        .option("header", "true")
        .load(file_name)
        .withColumn(
            "indicator_type", regexp_extract(input_file_name(), file_name_pattern, 1)
        )
        .withColumn("json_col", translate(col("values_per_year"), "'", '"'))
        .withColumn("map", from_json(col("json_col"), map_schema))
        .select(
            "country_code",
            explode(col("map")),
            lit(None).alias("unit"),
            "indicator_type",
        )
        .withColumnRenamed("key", "year")
        .withColumnRenamed("value", "value")
    )
    unemployment_rate.show(10)
    logger.info("Finished unemployment rate processing")

    logger.info("Starting purchasing power processing...")
    file_name = f"s3a://{DATA_BUCKET_NAME}/bronze/countries/indicators/{PROCESSED_DATE}/purchasing_power_parity.csv"
    file_name_pattern = ".*/(.*).csv"

    purchasing_power = (
        spark.read.format("csv")
        .option("header", "true")
        .load(file_name)
        .withColumn(
            "indicator_type", regexp_extract(input_file_name(), file_name_pattern, 1)
        )
        .withColumn("json_col", translate(col("values_per_year"), "'", '"'))
        .withColumn("map", from_json(col("json_col"), map_schema))
        .select(
            "country_code",
            explode(col("map")),
            lit(None).alias("unit"),
            "indicator_type",
        )
        .withColumnRenamed("key", "year")
        .withColumnRenamed("value", "value")
    )
    purchasing_power.show(10)
    logger.info("Finished purchasing power processing")

    logger.info("Starting job vacancies processing...")
    file_name = f"s3a://{DATA_BUCKET_NAME}/bronze/countries/indicators/{PROCESSED_DATE}/job_vacancies.csv"
    file_name_pattern = ".*/(.*).csv"

    job_vacancies = (
        spark.read.format("csv")
        .option("header", "true")
        .load(file_name)
        .withColumn(
            "indicator_type", regexp_extract(input_file_name(), file_name_pattern, 1)
        )
        .withColumn("year", concat(lit("20"), split(col("reference"), "/").getItem(1)))
        .select(
            "country_code",
            "year",
            col("last").alias("value"),
            lower(col("unit")).alias("unit"),
            "indicator_type",
        )
    )
    job_vacancies.show(10)
    logger.info("Finished job vacancies processing")

    all_indicators = (
        inflation.union(unemployment_rate).union(purchasing_power).union(job_vacancies)
    )
    all_indicators.show(10)

    all_indicators.groupBy("indicator_type").agg(count("*")).show(10)

    (
        all_indicators.withColumn("processed_date", lit(PROCESSED_DATE))
        .coalesce(1)
        .write.mode("overwrite")
        .options(header="True", delimiter=",")
        .partitionBy("country_code", "processed_date")
        .csv(f"s3a://{DATA_BUCKET_NAME}{TESTING_PREFIX}/silver/countries/indicators/")
    )

    logger.info("Finished indicators processing")


if __name__ == "__main__":
    main()
