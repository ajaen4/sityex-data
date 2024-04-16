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
    struct,
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
        "spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs",
        "false",
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

    logger.info("Finished reading listings")

    listings_count = listings.count()

    logger.info(f"Number of listings: {listings_count}")

    logger.info("Reading cities to show...")

    cities_to_show = (
        spark.read.options(header="True", delimiter=",")
        .csv(f"s3a://{DATA_BUCKET_NAME}/maps/cities_to_show.csv")
        .select("two_digit_postal_code", "city_id")
    )

    logger.info("Finished reading cities to show")

    logger.info("Unnesting facilities...")

    facilities_schema = listings.schema["facilities"].dataType.fields
    facility_names = [field.name for field in facilities_schema]

    new_facilities_cols = [
        col(f"facilities.{field_name}.value").alias(field_name)
        for field_name in facility_names
    ]
    listings = listings.withColumn("facilities", struct(*new_facilities_cols))

    logger.info("Unnested facilities")

    logger.info("Filtering listings...")

    spanish_listings = listings.where(
        col("location.countryCode") == "ES"
    ).withColumn(
        "two_digit_postal_code_listing",
        col("location.postalCode").substr(1, 2),
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

    logger.info("Adding partner field...")

    listings_partner = (
        listings_postal_code.withColumn("partner", lit("housing_anywhere"))
        .withColumn("housing_id", concat(lit("ha"), col("id")))
        .withColumn(
            "is_furnished",
            when(
                (col("facilities.roomFurniture") == "yes")
                | (col("facilities.bedroomFurnished") == "yes"),
                True,
            ).otherwise(False),
        )
        .drop("id", "__utId__")
        .withColumnRenamed("available", "availability")
        .withColumnRenamed("originalLink", "link")
    )

    logger.info("Added partner field")

    logger.info("Writing listings...")

    (
        listings_partner.write.mode("overwrite")
        .option("multiLine", True)
        .partitionBy("city_id")
        .json(
            f"s3a://{DATA_BUCKET_NAME}{TESTING_PREFIX}/silver/partners/housing_anywhere/{PROCESSED_DATE_HOUSING}/"
        )
    )

    logger.info("Finished writing listings")


if __name__ == "__main__":
    main()
