import sys

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import unicodedata
from datetime import datetime
import re

from logger import logger

from pyspark.sql.functions import (
    col,
    count,
    explode,
    split,
    collect_set,
    concat_ws,
    concat,
    trim,
    udf,
    regexp_replace,
    when,
    array,
    lit,
    to_date,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    IntegerType,
)

from deep_translator import GoogleTranslator


def main():

    def remove_diacritics(text: str):
        nfkd_form = unicodedata.normalize("NFKD", text)
        ascii_string = nfkd_form.encode("ASCII", "ignore")
        return ascii_string.decode("utf-8")

    remove_diacritics_udf = udf(remove_diacritics, StringType())

    def add_newlines(text):
        # Regular expression for emojis
        emoji_pattern = re.compile(
            "["
            "\U0001F600-\U0001F64F"  # emoticons
            "\U0001F300-\U0001F5FF"  # symbols & pictographs
            "\U0001F680-\U0001F6FF"  # transport & map symbols
            "\U0001F700-\U0001F77F"  # alchemical symbols
            "\U0001F780-\U0001F7FF"  # Geometric Shapes Extended
            "\U0001F800-\U0001F8FF"  # Supplemental Arrows-C
            "\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
            "\U0001FA00-\U0001FA6F"  # Chess Symbols
            "\U0001FA70-\U0001FAFF"  # Symbols and Pictographs Extended-A
            "\U00002702-\U000027B0"  # Dingbats
            "\U000024C2-\U0001F251"
            "⏳"
            "]+",
            flags=re.UNICODE,
        )

        phrase_pattern = re.compile(
            r"Que vas a disfrutar|Información( adicional)?:?|Descripción|Intérpretes?:?|Programa Candlelight[a-zA-Z0-9 ]*:|Opiniones.*💬"
        )

        text = phrase_pattern.sub(r"\n\n\g<0>\n", text)
        text = emoji_pattern.sub(r"\n\g<0>", text)

        return text

    add_newlines_udf = udf(add_newlines, StringType())

    def translate_to_english(text):
        if text and len(text.strip()) > 0:
            try:
                return GoogleTranslator(source='es', target='en').translate(text)
            except Exception as e:
                logger.error(f"Error in translation: {e}")
                return text
        else:
            return text

    translate_udf = udf(translate_to_english, StringType())

    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "DATA_BUCKET_NAME",
            "PROCESSED_DATE_CITIES",
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
    PROCESSED_DATE_CITIES = args["PROCESSED_DATE_CITIES"]

    now = datetime.now()
    PROCESSED_DATE_FEVER = now.strftime("%d-%m-%Y")

    if args.get("TESTING_PREFIX") != "None":
        TESTING_PREFIX = args["TESTING_PREFIX"]
    else:
        TESTING_PREFIX = ""

    logger.info(f"DATA_BUCKET_NAME: {DATA_BUCKET_NAME}")
    logger.info(f"PROCESSED_DATE_CITIES: {PROCESSED_DATE_CITIES}")
    logger.info(f"PROCESSED_DATE_FEVER: {PROCESSED_DATE_FEVER}")
    logger.info(f"TESTING_PREFIX: {TESTING_PREFIX}")

    logger.info("Starting Fever data processing...")

    logger.info("Reading cities in Spain...")

    cities_paths = f"s3a://{DATA_BUCKET_NAME}/silver/cities/geographical/all_cities/{PROCESSED_DATE_CITIES}/"
    spain_cities = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(cities_paths)
        .select(
            col("geonameid").alias("city_id"),
            remove_diacritics_udf(col("name")).alias("city_name"),
        )
        .where(col("country_3_code") == "ESP")
    )

    logger.info("Finished reading cities in Spain")

    logger.info("Reading Fever catalog...")

    catalog_path = f"s3a://{DATA_BUCKET_NAME}/bronze/partners/fever/{PROCESSED_DATE_FEVER}/events_catalog.csv"

    schema = StructType(
        [
            StructField("country", StringType(), True),
            StructField("state", StringType(), True),
            StructField("city", StringType(), True),
            StructField("venue", StringType(), True),
            StructField("location", StringType(), True),
            StructField("coordinates", StringType(), True),
            StructField("sku", IntegerType(), True),
            StructField("plan_name", StringType(), True),
            StructField("affiliate_url", StringType(), True),
            StructField("fever_url", StringType(), True),
            StructField("description", StringType(), True),
            StructField("plan_rating", IntegerType(), True),
            StructField("start_date", StringType(), True),
            StructField("end_date", StringType(), True),
            StructField("sessions", StringType(), True),
            StructField("remaining_days", IntegerType(), True),
            StructField("minimum_price", FloatType(), True),
            StructField("currency", StringType(), True),
            StructField("ticket_prices", StringType(), True),
            StructField("availability_of_tickets", StringType(), True),
            StructField("category", StringType(), True),
            StructField("subcategory", StringType(), True),
            StructField("additional_notes", StringType(), True),
            StructField("photo_1", StringType(), True),
            StructField("photo_2", StringType(), True),
        ]
    )

    catalog = (
        spark.read.csv(
            catalog_path,
            header=True,
            schema=schema,
            # fmt: off
            quote="\"",
            escape="\"",
            # fmt: on
            sep=",",
            inferSchema=False,
        )
        .withColumn("start_date", to_date(col("start_date"), "M/d/yyyy"))
        .withColumn("end_date", to_date(col("end_date"), "M/d/yyyy"))
        .withColumn("city", remove_diacritics_udf(regexp_replace("city", "-", "/")))
        .withColumn(
            "city", regexp_replace("city", "Tenerife", "Santa Cruz de Tenerife")
        )
        .withColumn("city", regexp_replace("city", "Majorca", "Palma"))
        .withColumn("coordinates", regexp_replace("coordinates", "[()]", ""))
        .withColumn("latitude", split(col("coordinates"), ";").getItem(0).cast("float"))
        .withColumn(
            "longitude", split(col("coordinates"), ";").getItem(1).cast("float")
        )
        .drop("coordinates")
    )

    logger.info("Duplicated SKUs:")
    catalog.groupBy("sku").agg(count("*").alias("count_sku")).where(
        col("count_sku") > 1
    ).show()

    logger.info("Finished reading Fever catalog")

    logger.info("Starting data processing...")

    subcategories = (
        catalog.select(explode(split(col("subcategory"), ",")).alias("subcategory"))
        .select(trim(col("subcategory")).alias("subcategory"))
        .distinct()
        .orderBy("subcategory")
    )
    subcategories.show(10)

    (
        subcategories.coalesce(1)
        .write.mode("overwrite")
        .options(header="True", delimiter=",")
        .csv(
            f"s3a://{DATA_BUCKET_NAME}{TESTING_PREFIX}/silver/partners/fever/subcategories/{PROCESSED_DATE_FEVER}/"
        )
    )

    spain_catalog = catalog.where(col("country") == "Spain")
    distinct_cities_count = spain_catalog.select("city").distinct().count()
    logger.info(f"Distinct cities in Spain's catalog: {distinct_cities_count}")

    spain_catalog_city_ids = spain_catalog.join(
        spain_cities,
        col("city") == col("city_name"),
        "left",
    )

    logger.info("No matches for cities in catalog:")
    no_match = spain_catalog_city_ids.where(col("city_id").isNull()).select(
        "sku", "city", "city_name", "plan_name"
    )
    no_match.show(100)
    no_match.count()

    spain_catalog_city_ids = spain_catalog_city_ids.drop(
        "country", "state", "city", "city_name"
    )

    subcategories_map_path = f"s3a://{DATA_BUCKET_NAME}/maps/subcategories_map.csv"
    subcategories_map = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(subcategories_map_path)
    )
    subcategories_map.show()

    catalog_cols = spain_catalog_city_ids.columns
    catalog_cols.remove("subcategory")
    sityex_subcategories = (
        spain_catalog_city_ids.withColumn(
            "subcategory",
            when(col("subcategory").isNull(), array(lit(""))).otherwise(
                split(col("subcategory"), ",")
            ),
        )
        .select(*catalog_cols, explode(col("subcategory")).alias("subcategory"))
        .join(subcategories_map, col("subcategory") == col("fever_subcategory"), "left")
        .groupBy("sku")
        .agg(
            collect_set(col("subcategory")).alias("subcategories"),
            collect_set(col("sityex_subcategory")).alias("sityex_subcategories"),
        )
        .withColumn("subcategories", concat_ws(",", "subcategories"))
        .withColumn("sityex_subcategories", concat_ws(",", "sityex_subcategories"))
        .withColumn(
            "sityex_subcategories",
            when(col("sityex_subcategories") == "", lit("Other")).otherwise(
                col("sityex_subcategories")
            ),
        )
    )

    catalog_with_sityex_subcategories = (
        spain_catalog_city_ids.withColumnRenamed("sku", "sku_org")
        .join(
            sityex_subcategories,
            col("sku_org") == col("sku"),
            "left",
        )
        .drop("sku_org", "subcategory")
    )

    logger.info("Adding new lines to descriptions...")

    catalog_with_formatted_descs = (
        catalog_with_sityex_subcategories.withColumn(
            "description", add_newlines_udf(col("description"))
        )
    )

    logger.info("Added new lines to descriptions")

    logger.info("Translating descriptions...")

    catalog_translated = (
        catalog_with_formatted_descs
        .withColumn("description_en", translate_udf(col("description")))
        .withColumnRenamed("description", "description_es")
    )

    logger.info("Translated descriptions")

    logger.info("Adding partner field...")

    catalog_with_partner = (
        catalog_translated
        .withColumn("partner", lit("fever"))
        .withColumn("sku", concat(lit("f"), col("sku")))
    )

    logger.info("Added partner field")

    (
        catalog_with_partner.coalesce(1).write.mode("overwrite")
        # fmt: off
        .options(header="True", delimiter=",", quote="\"", escape="\"")
        # fmt: on
        .csv(
            f"s3a://{DATA_BUCKET_NAME}{TESTING_PREFIX}/silver/partners/fever/catalog/{PROCESSED_DATE_FEVER}/"
        )
    )

    logger.info("Finished Fever data processing")


if __name__ == "__main__":
    main()
