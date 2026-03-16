from pyspark.sql import functions as F
from pyspark.sql.window import Window
from config import TRANSACTIONS_PATH, MERCHANTS_PATH


def load_transactions(spark):
    # loads parquet and parses purchase_date string to timestamp, extracting time columns
    df = spark.read.parquet(TRANSACTIONS_PATH)
    df = (
        df
        .withColumn("purchase_date", F.to_timestamp("purchase_date"))
        .withColumn("month", F.date_format("purchase_date", "MMM yyyy"))
        .withColumn("month_sort", F.date_format("purchase_date", "yyyy-MM"))
        .withColumn("year", F.year("purchase_date"))
        .withColumn("month_num", F.month("purchase_date"))
        .withColumn("hour", F.hour("purchase_date"))
    )
    return df


def load_merchants(spark):
    # reads merchants CSV and deduplicates by keeping the record with best sales range per merchant_id
    # assumption: when a merchant_id has multiple rows with diferent data, we trust the one
    # with the highest most_recent_sales_range (A is best, E is worst) as the most relevant record
    df = spark.read.csv(MERCHANTS_PATH, header=True, inferSchema=True)

    sales_rank = (
        F.when(F.col("most_recent_sales_range") == "A", 1)
        .when(F.col("most_recent_sales_range") == "B", 2)
        .when(F.col("most_recent_sales_range") == "C", 3)
        .when(F.col("most_recent_sales_range") == "D", 4)
        .otherwise(5)
    )

    w = Window.partitionBy("merchant_id").orderBy(sales_rank)

    df = (
        df
        .withColumn("_rank", F.row_number().over(w))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )
    return df


def build_joined(spark):
    # joins transactions with merchants, renaming conflicting geo columns and aplying cleaning rules
    transactions = load_transactions(spark)
    merchants = load_merchants(spark)

    merchants_clean = (
        merchants
        .withColumnRenamed("city_id", "merchant_city_id")
        .withColumnRenamed("state_id", "merchant_state_id")
        .drop("merchant_category_id", "subsector_id")
        .select(
            "merchant_id",
            "merchant_name",
            "merchant_city_id",
            "merchant_state_id",
            "merchant_group_id",
            "most_recent_sales_range",
            "most_recent_purchases_range",
            "numerical_1",
            "numerical_2",
            "avg_sales_lag3", "avg_purchases_lag3", "active_months_lag3",
            "avg_sales_lag6", "avg_purchases_lag6", "active_months_lag6",
            "avg_sales_lag12", "avg_purchases_lag12", "active_months_lag12",
        )
    )

    joined = transactions.join(merchants_clean, on="merchant_id", how="left")

    joined = joined.withColumn(
        "merchant_name",
        F.when(F.col("merchant_name").isNull(), F.col("merchant_id").cast("string"))
        .otherwise(F.col("merchant_name"))
    )

    joined = joined.withColumn(
        "category",
        F.when(F.col("category").isNull(), F.lit("Unknown category"))
        .otherwise(F.col("category"))
    )

    return joined


def load_all(spark):
    # convenience wrapper that retuns both raw merchants and the cleaned joined df
    merchants = load_merchants(spark)
    joined = build_joined(spark)
    return joined, merchants
