from pyspark.sql import functions as F
from pyspark.sql.window import Window


def run(df):
    # ranks top 5 merchants by total purchase amount for each month and city combination
    agg = (
        df
        .groupBy("month", "month_sort", "city_id", "merchant_name")
        .agg(
            F.sum("purchase_amount").alias("purchase_total"),
            F.count("*").alias("no_of_sales"),
        )
    )

    window = Window.partitionBy("month", "city_id").orderBy(F.col("purchase_total").desc())

    result = (
        agg
        # dense_rank keeps ties visible instead of cutting them
        .withColumn("rank", F.dense_rank().over(window))
        .filter(F.col("rank") <= 5)
        .orderBy("month_sort", "city_id", "rank")
        .select(
            F.col("month").alias("Month"),
            F.col("city_id").alias("City"),
            F.col("merchant_name").alias("Merchant"),
            F.round(F.col("purchase_total"), 2).alias("Purchase Total"),
            F.col("no_of_sales").alias("No of sales"),
        )
    )

    return result
