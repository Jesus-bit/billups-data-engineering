from pyspark.sql import functions as F
from pyspark.sql.window import Window


def run(df):
    # finds top 3 hours by total purchase amount for each product category using window ranking
    agg = (
        df
        .groupBy("category", "hour")
        .agg(F.sum("purchase_amount").alias("total_amount"))
    )

    window = Window.partitionBy("category").orderBy(F.col("total_amount").desc())

    hour_fmt = F.concat(F.lpad(F.col("hour").cast("string"), 2, "0"), F.lit("00"))

    result = (
        agg
        # dense_rank keeps ties visible instead of cutting them
        .withColumn("rank", F.dense_rank().over(window))
        .filter(F.col("rank") <= 3)
        .withColumn("hour_fmt", hour_fmt)
        .orderBy("category", "hour_fmt")
        .select(
            F.col("category").alias("Product Category"),
            F.col("hour_fmt").alias("Hour"),
        )
    )

    return result
