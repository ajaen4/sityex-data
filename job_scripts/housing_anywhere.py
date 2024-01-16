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

    logger.info("Reading cities to show...")

    cities_to_show = (
        spark.read.options(header="True", delimiter=",")
        .csv(f"s3a://{DATA_BUCKET_NAME}/maps/cities_to_show.csv")
        .select("postal_code", "city_id")
    )

    logger.info("Finished reading cities to show")

    logger.info("Filtering listings...")

    spanish_listings = listings.where(col("location.countryCode") == "ES")
    num_spanish_list = spanish_listings.count()
    logger.info(f"Number of Spanish listings: {num_spanish_list}")

    listings_postal_code = (
        spanish_listings.join(
            cities_to_show,
            spanish_listings["location.postalCode"] == cities_to_show["postal_code"],
            "left",
        )
        .where(col("postal_code").isNotNull())
        .drop("postal_code")
    )

    logger.info("Finished Filtering listings")

    logger.info("Writing listings...")

    (
        listings_postal_code.write.mode("overwrite")
        .option("multiLine", True)
        .partitionBy("city_id")
        .json(
            f"s3a://{DATA_BUCKET_NAME}{TESTING_PREFIX}/silver/partners/housing_anywhere/{PROCESSED_DATE_HOUSING}/"
        )
    )

    logger.info("Finished writing listings")


if __name__ == "__main__":
    main()
