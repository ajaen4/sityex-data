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
    concat,
    split,
    lower,
)


def main():
    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "DATA_BUCKET_NAME", "PROCESSED_DATE", "TESTING_PREFIX"],
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

    logger.info(f"DATA_BUCKET_NAME: {DATA_BUCKET_NAME}")
    logger.info(f"PROCESSED_DATE: {PROCESSED_DATE}")
    logger.info(f"TESTING_PREFIX: {TESTING_PREFIX}")

    logger.info("Starting taxes processing...")

    file_name = (
        f"s3a://{DATA_BUCKET_NAME}/bronze/countries/taxes/{PROCESSED_DATE}/"
    )
    file_name_pattern = ".*/(.*).csv"

    taxes = (
        spark.read.format("csv")
        .option("header", "true")
        .load(file_name)
        .withColumn(
            "tax_type", regexp_extract(input_file_name(), file_name_pattern, 1)
        )
        .withColumn(
            "year", concat(lit("20"), split(col("reference"), "/").getItem(1))
        )
        .select(
            "country_code",
            "year",
            col("last").alias("value"),
            lower(col("unit")).alias("unit"),
            "tax_type",
        )
    )
    taxes.show(10)

    taxes.groupBy("tax_type").agg(count("*")).show(10)

    (
        taxes.withColumn("processed_date", lit(PROCESSED_DATE))
        .coalesce(1)
        .write.mode("overwrite")
        .options(header="True", delimiter=",")
        .partitionBy("country_code", "processed_date")
        .csv(
            f"s3a://{DATA_BUCKET_NAME}{TESTING_PREFIX}/silver/countries/taxes/"
        )
    )

    logger.info("Finished taxes processing")


if __name__ == "__main__":
    main()
