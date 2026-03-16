from pyspark.sql import functions as F


def run(df):
    # calculates avg purchase amount per merchant per state and retuns sorted descending
    result = (
        df
        .groupBy("merchant_name", "state_id")
        .agg(F.avg("purchase_amount").alias("average_amount"))
        .orderBy(F.col("average_amount").desc())
        .select(
            F.col("merchant_name").alias("Merchant"),
            F.col("state_id").alias("State ID"),
            F.round(F.col("average_amount"), 2).alias("Average Amount"),
        )
    )
    return result
