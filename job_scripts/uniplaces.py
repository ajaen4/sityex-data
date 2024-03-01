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
    concat,
    when,
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
    PROCESSED_DATE_UNI = now.strftime("%d-%m-%Y")

    logger.info("Reading listings...")

    listings = (
        spark.read.option("header", "true")
        .option("multiline", "true")
        .option("escape", '"')
        .option("quote", '"')
        .csv(
            f"s3a://{DATA_BUCKET_NAME}/bronze/partners/uniplaces/{PROCESSED_DATE_UNI}/"
        )
    )

    logger.info("Finished reading listings")

    logger.info("Reading cities to show...")

    cities_to_show = (
        spark.read.options(header="True", delimiter=",")
        .csv(f"s3a://{DATA_BUCKET_NAME}/maps/cities_to_show.csv")
        .select("two_digit_postal_code", "city_id")
    )

    logger.info("Finished reading cities to show")

    logger.info("Filtering listings...")

    spanish_listings = listings.where(col("country") == "Spain").withColumn(
        "two_digit_postal_code_listing", col("postcode").substr(1, 2)
    )
    num_spanish_list = spanish_listings.count()
    logger.info(f"Number of Spanish listings: {num_spanish_list}")

    listings_postal_code = (
        spanish_listings.join(
            cities_to_show,
            spanish_listings["two_digit_postal_code_listing"]
            == cities_to_show["two_digit_postal_code"],
            "left",
        )
        .where(col("two_digit_postal_code").isNotNull())
        .drop("two_digit_postal_code_listing", "two_digit_postal_code")
    )

    logger.info("Finished Filtering listings")

    logger.info("Adding and modifying fields...")

    extra_fields = (
        listings_postal_code.withColumn("partner", lit("uniplaces"))
        .withColumn("housing_id", concat(lit("un"), col("id")))
        .withColumn(
            "link",
            concat(col("url"), lit("/?mtm_campaign=SityEx&mtm_source=Affiliate")),
        )
        .drop("url")
        .withColumn(
            "kindLabel",
            when(col("type") == "Rooms", "private room")
            .when(col("type") == "Apartment", "entire place")
            .when(col("type") == "Beds", "private room"),
        )
        .withColumn(
            "costsFormatted_price",
            concat(col("price").cast("string"), lit(".00")),
        )
        .withColumn("rank", lit(0))
        .withColumnRenamed(
            "currency_code",
            "costsFormatted_currency",
        )
        .withColumnRenamed("picture_urls", "images")
        .withColumnRenamed("content", "description")
        .withColumn(
            "description",
            when(col("description").isNull(), "").otherwise(col("description")),
        )
    )

    logger.info("Finished adding and modifying fields")

    logger.info("Writing listings...")

    (
        extra_fields.write.mode("overwrite")
        .option("multiLine", True)
        .partitionBy("city_id")
        .json(
            f"s3a://{DATA_BUCKET_NAME}{TESTING_PREFIX}/silver/partners/uniplaces/{PROCESSED_DATE_UNI}/"
        )
    )

    logger.info("Finished writing listings")


if __name__ == "__main__":
    main()
