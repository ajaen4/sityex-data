import sys

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from logger import logger

from pyspark.sql.functions import (
    col,
)
from datetime import datetime


def main():
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "DATA_BUCKET_NAME",
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

    if args.get("TESTING_PREFIX") != "None":
        TESTING_PREFIX = args["TESTING_PREFIX"]
    else:
        TESTING_PREFIX = ""

    logger.info(f"DATA_BUCKET_NAME: {DATA_BUCKET_NAME}")
    logger.info(f"TESTING_PREFIX: {TESTING_PREFIX}")

    now = datetime.now()
    PROCESSED_DATE_HOUSING = now.strftime("%d-%m-%Y")

    logger.info("Reading listings...")

    listings = spark.read.option("multiLine", True).json(
        f"s3a://{DATA_BUCKET_NAME}/bronze/partners/housing_anywhere/{PROCESSED_DATE_HOUSING}/"
    )

    logger.info("Finished reading listings...")

    listings_count = listings.count()

    logger.info(f"Number of listings: {listings_count}")

    logger.info("Filtering listings...")

    spanish_listings = listings.where(col("location.countryCode") == "ES")
    num_spanish_list = spanish_listings.count()
    logger.info(f"Number of Spanish listings: {num_spanish_list}")

    logger.info("Writing listings...")

    (
        spanish_listings.write.mode("overwrite")
        .option("multiLine", True)
        .json(
            f"s3a://{DATA_BUCKET_NAME}{TESTING_PREFIX}/silver/partners/housing_anywhere/{PROCESSED_DATE_HOUSING}/"
        )
    )

    logger.info("Finished writing listings")


if __name__ == "__main__":
    main()
