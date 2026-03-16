from pyspark.sql import functions as F
from pyspark.sql.window import Window


def _popular_merchants(df):
    # ranks merchants by transaction count and shows which city each one is located in
    result = (
        df
        .groupBy("merchant_name", "merchant_city_id")
        .agg(F.count("*").alias("total_transactions"))
        .orderBy(F.col("total_transactions").desc())
        .select(
            F.col("merchant_name").alias("Merchant"),
            F.col("merchant_city_id").alias("Merchant City"),
            F.col("total_transactions").alias("Total Transactions"),
        )
    )
    return result


def _city_summary(df):
    # aggregates transaction volume by merchant city to see which cities concentrate popular merchants
    result = (
        df
        .groupBy("merchant_city_id")
        .agg(
            F.countDistinct("merchant_name").alias("total_merchants"),
            F.count("*").alias("total_transactions"),
        )
        .orderBy(F.col("total_transactions").desc())
        .select(
            F.col("merchant_city_id").alias("Merchant City"),
            F.col("total_merchants").alias("Total Merchants"),
            F.col("total_transactions").alias("Total Transactions"),
        )
    )
    return result


def _city_category_correlation(df):
    # shows transaction count per city + category and adds a pct column to quantify concentration
    city_totals = (
        df
        .groupBy("merchant_city_id")
        .agg(F.count("*").alias("city_total"))
    )

    city_cat = (
        df
        .groupBy("merchant_city_id", "category")
        .agg(F.count("*").alias("tx_count"))
    )

    result = (
        city_cat
        .join(city_totals, on="merchant_city_id", how="left")
        .withColumn("pct_of_city", F.round(F.col("tx_count") / F.col("city_total") * 100, 2))
        .orderBy("merchant_city_id", F.col("tx_count").desc())
        .select(
            F.col("merchant_city_id").alias("Merchant City"),
            F.col("category").alias("Category"),
            F.col("tx_count").alias("Transaction Count"),
            F.col("pct_of_city").alias("% of City Total"),
        )
    )
    return result


def run(df):
    # anlyzes merchant popularity by city location and explores city-category concentration
    popular_merchants = _popular_merchants(df)
    city_summary = _city_summary(df)
    city_category = _city_category_correlation(df)
    return popular_merchants, city_summary, city_category
