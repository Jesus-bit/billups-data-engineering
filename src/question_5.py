import os
import matplotlib
matplotlib.use("Agg")  # no display needed
import matplotlib.pyplot as plt
from pyspark.sql import functions as F
from question_4 import run as q4_run

MONTHLY_DEFAULT_RATE = 0.229  # 22.9% chance of default per month, accumulates with more installments
GROSS_MARGIN = 0.25           # 25% of revenue is gross profit for the merchant
# defaulters pay floor(N/2) installments before stopping


def best_cities(df):
    # reuses q4 city summary — already ranks cities by transaction volume
    _, city_summary, _ = q4_run(df)
    return city_summary


def best_categories(df):
    # ranks categories by total revenue, filters Unknown since we cant recommend selling unknown stuff
    result = (
        df
        .filter(F.col("category") != "Unknown category")
        .groupBy("category")
        .agg(
            F.count("*").alias("Transactions"),
            F.round(F.sum("purchase_amount"), 2).alias("Total Revenue"),
            F.round(F.avg("purchase_amount"), 2).alias("Avg Ticket"),
        )
        .orderBy(F.col("Total Revenue").desc())
        .select(
            F.col("category").alias("Category"),
            F.col("Transactions"),
            F.col("Total Revenue"),
            F.col("Avg Ticket"),
        )
    )
    return result


def interesting_months(df):
    # monthly totals ordered chronologicaly to spot seasonal patterns
    result = (
        df
        .groupBy("month_sort", "month")
        .agg(
            F.count("*").alias("Transactions"),
            F.round(F.sum("purchase_amount"), 2).alias("Total Revenue"),
            F.round(F.avg("purchase_amount"), 2).alias("Avg Ticket"),
        )
        .orderBy("month_sort")
        .select(
            F.col("month").alias("Month"),
            F.col("Transactions"),
            F.col("Total Revenue"),
            F.col("Avg Ticket"),
        )
    )
    return result


def recommended_hours(df):
    # hourly distribution to find the best open-close window for a new merchant
    total_tx = df.count()
    result = (
        df
        .groupBy("hour")
        .agg(
            F.count("*").alias("Transactions"),
            F.round(F.sum("purchase_amount"), 2).alias("Total Revenue"),
        )
        .withColumn("Pct of Total", F.round(F.col("Transactions") / F.lit(total_tx) * 100, 2))
        .orderBy("hour")
        .select(
            F.col("hour").alias("Hour"),
            F.col("Transactions"),
            F.col("Total Revenue"),
            F.col("Pct of Total"),
        )
    )
    return result


def _save_margin_chart(detail_pd, output_path):
    # saves a line chart of effective margin % by installment count
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(detail_pd["Installments"], detail_pd["Effective Margin %"], marker="o", linewidth=2)
    ax.axhline(0, color="red", linestyle="--", linewidth=1, label="Break-even")
    ax.set_title("Effective Profit Margin by Number of Installments")
    ax.set_xlabel("Installments")
    ax.set_ylabel("Effective Margin %")
    ax.legend()
    ax.grid(True, alpha=0.3)
    os.makedirs(output_path, exist_ok=True)
    fig.savefig(os.path.join(output_path, "q5e_installment_margin_chart.png"), bbox_inches="tight")
    plt.close(fig)


def installment_analysis(df, output_path="output"):
    # models expected profit using cumulative monthly default probability, single payments have no risk
    clean = df.filter(F.col("installments") != 999)

    n = F.col("installments")
    p = F.col("purchase_amount")

    # cumulative default prob: 1 - (1 - 0.229)^N — grows with each added installment
    cumulative_default = 1 - F.pow(F.lit(1 - MONTHLY_DEFAULT_RATE), n)

    paid_if_default = (F.floor(n / 2) / n) * p
    loss_if_default = paid_if_default - (p * (1 - GROSS_MARGIN))

    expected_profit = F.when(
        n <= 1,
        p * GROSS_MARGIN  # single payment: no default risk
    ).otherwise(
        ((1 - cumulative_default) * p * GROSS_MARGIN) + (cumulative_default * loss_if_default)
    )

    with_profit = (
        clean
        .withColumn("cumulative_default", cumulative_default)
        .withColumn("expected_profit", expected_profit)
    )

    # summary: single vs installment
    summary = (
        with_profit
        .withColumn(
            "payment_type",
            F.when(F.col("installments") <= 1, F.lit("single")).otherwise(F.lit("installment"))
        )
        .groupBy("payment_type")
        .agg(
            F.count("*").alias("Transactions"),
            F.round(F.sum("purchase_amount"), 2).alias("Total Revenue"),
            F.round(F.avg("purchase_amount"), 2).alias("Avg Ticket"),
            F.round(F.sum("expected_profit"), 2).alias("Total Expected Profit"),
            F.round(F.avg("expected_profit"), 2).alias("Avg Expected Profit"),
        )
        .orderBy("payment_type")
        .select(
            F.col("payment_type").alias("Payment Type"),
            F.col("Transactions"),
            F.col("Total Revenue"),
            F.col("Avg Ticket"),
            F.col("Total Expected Profit"),
            F.col("Avg Expected Profit"),
        )
    )

    # detail: one row per installment count, includes cumulative default rate for visibility
    detail = (
        with_profit
        .groupBy("installments")
        .agg(
            F.count("*").alias("Transactions"),
            F.round(F.avg("purchase_amount"), 2).alias("Avg Ticket"),
            F.round(F.avg("cumulative_default") * 100, 2).alias("Cumulative Default %"),
            F.round(F.avg("expected_profit"), 2).alias("Avg Expected Profit"),
            F.round(F.avg("expected_profit") / F.avg("purchase_amount") * 100, 2).alias("Effective Margin %"),
        )
        .orderBy("installments")
        .select(
            F.col("installments").alias("Installments"),
            F.col("Transactions"),
            F.col("Avg Ticket"),
            F.col("Cumulative Default %"),
            F.col("Avg Expected Profit"),
            F.col("Effective Margin %"),
        )
    )

    detail_pd = detail.toPandas()
    _save_margin_chart(detail_pd, output_path)

    # re-create detail as spark df from pandas so caller gets a consistent type
    spark = clean.sparkSession
    detail = spark.createDataFrame(detail_pd)

    return summary, detail


def run(df):
    # data-driven advice for a new merchant, reuses previous question outputs where posible
    cities = best_cities(df)
    categories = best_categories(df)
    months = interesting_months(df)
    hours = recommended_hours(df)
    installments_summary, installments_detail = installment_analysis(df)
    return cities, categories, months, hours, installments_summary, installments_detail
