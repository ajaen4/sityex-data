import sys

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime
import re

from logger import logger

from pyspark.sql.functions import (
    col,
    concat,
    udf,
    lit,
)
from pyspark.sql.types import (
    StringType,
)


def main():
    def text_to_html(text):
        text = text.replace("\n", "<br/>")
        text = re.sub(r"\*\*(.*?)\*\*", r"<b>\1</b>", text)
        return text

    text_to_html_udf = udf(text_to_html, StringType())

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

    now = datetime.now()
    PROCESSED_DATE_MEETUP = now.strftime("%d-%m-%Y")

    if args.get("TESTING_PREFIX") != "None":
        TESTING_PREFIX = args["TESTING_PREFIX"]
    else:
        TESTING_PREFIX = ""

    logger.info(f"DATA_BUCKET_NAME: {DATA_BUCKET_NAME}")
    logger.info(f"PROCESSED_DATE_MEETUP: {PROCESSED_DATE_MEETUP}")
    logger.info(f"TESTING_PREFIX: {TESTING_PREFIX}")

    logger.info("Starting Meetup data processing...")

    logger.info("Reading Meetup catalog...")

    catalog_path = f"s3a://{DATA_BUCKET_NAME}/bronze/partners/meetup/{PROCESSED_DATE_MEETUP}/events_catalog.csv"

    catalog = (
        spark.read.option("header", "true")
        .option("multiline", "true")
        .option("escape", '"')
        .option("quote", '"')
        .csv(
            catalog_path,
        )
    )

    logger.info("Finished reading Fever catalog")

    logger.info("Starting data processing...")

    catalog_with_html_desc = catalog.withColumn(
        "description_html", text_to_html_udf(col("description"))
    )

    logger.info("Added new lines to descriptions")

    logger.info("Adding partner field...")

    catalog_with_partner = catalog_with_html_desc.withColumn(
        "partner", lit("meetup")
    ).withColumn("event_id", concat(lit("m"), col("event_id")))

    logger.info("Added partner field")

    (
        catalog_with_partner.coalesce(1)
        .write.mode("overwrite")
        .options(
            header="True",
            delimiter=",",  # fmt: off
            quote='"',
            escape='"',
            # fmt: on
        )
        .csv(
            f"s3a://{DATA_BUCKET_NAME}{TESTING_PREFIX}/silver/partners/meetup/{PROCESSED_DATE_MEETUP}/"
        )
    )

    logger.info("Finished Meetup data processing")


if __name__ == "__main__":
    main()
