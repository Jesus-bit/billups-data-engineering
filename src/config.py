from pyspark.sql import SparkSession

# paths to raw data files
TRANSACTIONS_PATH = "data/part-00000-tid-860771939793626614-979f966a-6d53-4896-9692-f81194d27b99-109986-1-c000.snappy.parquet"
MERCHANTS_PATH = "data/merchants-subset.csv"

# output directory for results
OUTPUT_PATH = "output"


def get_spark():
    # builds a local SparkSession with enough memory for the datasets
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("billups-data-engineering")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
