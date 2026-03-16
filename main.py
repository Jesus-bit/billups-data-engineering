import sys
sys.path.insert(0, "src")

from config import get_spark, OUTPUT_PATH
from data_loader import load_all
from question_1 import run as q1
from question_2 import run as q2
from question_3 import run as q3
from question_4 import run as q4
from question_5 import run as q5


def save(df, name):
    # writes a single CSV file (coalesce avoids multiple part files)
    path = f"{OUTPUT_PATH}/{name}"
    df.coalesce(1).write.csv(path, header=True, mode="overwrite")
    print(f"  saved -> {path}")


def main():
    print("Starting Billups Data Engineering analysis...")
    spark = get_spark()

    df, _ = load_all(spark)
    df.cache()

    # Q1: top 5 merchants per month and city
    print("\n[Q1] Top 5 merchants by purchase amount per month and city")
    q1_result = q1(df)
    q1_result.show(20, truncate=False)
    save(q1_result, "question_1")

    # Q2: avg purchase amount per merchant per state
    print("\n[Q2] Average purchase amount per merchant per state")
    q2_result = q2(df)
    q2_result.show(20, truncate=False)
    save(q2_result, "question_2")

    # Q3: top 3 peak hours per product category
    print("\n[Q3] Top 3 hours by purchase amount per category")
    q3_result = q3(df)
    q3_result.show(20, truncate=False)
    save(q3_result, "question_3")

    # Q4: popular merchants by city + city-category corelation
    print("\n[Q4] Popular merchants by city and category correlation")
    q4_popular, q4_cities, q4_correlation = q4(df)
    q4_popular.show(20, truncate=False)
    q4_cities.show(20, truncate=False)
    q4_correlation.show(20, truncate=False)
    save(q4_popular, "question_4_popular")
    save(q4_cities, "question_4_cities")
    save(q4_correlation, "question_4_correlation")

    # Q5: recommendations for a new merchant
    print("\n[Q5] Recommendations for a new merchant")
    q5_cities, q5_categories, q5_months, q5_hours, q5_inst_summary, q5_inst_detail = q5(df)
    q5_cities.show(20, truncate=False)
    q5_categories.show(truncate=False)
    q5_months.show(20, truncate=False)
    q5_hours.show(24, truncate=False)
    q5_inst_summary.show(truncate=False)
    q5_inst_detail.show(truncate=False)
    save(q5_inst_summary, "question_5_installments_summary")
    save(q5_inst_detail, "question_5_installments_detail")
    # chart saved to output/q5e_installment_margin_chart.png by question_5.py

    df.unpersist()
    spark.stop()
    print("\ndone — check output/ for results")


if __name__ == "__main__":
    main()
