# Billups Data Engineering — Technical Assessment

PySpark analysis of merchant transaction data. Answers 5 analytical questions about merchant performance, city trends, and recommendations for new merchants.

## Setup

Python 3.9+ and Java 11+ required (Spark needs Java).

```bash
pip install -r requirements.txt
```

## Data

Put the raw files inside `data/`:

```
data/
├── historical_transactions.parquet
└── merchants.csv
```

These are not included in the repo (too large).

## Run

```bash
python main.py
```

Results get saved to `output/` as CSVs. Each question has its own subfolder.

## Questions covered

- **Q1** — Top 5 merchants by purchase volume for each month + city
- **Q2** — Avg purchase amount per merchant per state
- **Q3** — Top 3 hours of day with highest sales per product category
- **Q4** — Most popular merchants by city + city/category correlation
- **Q5** — Recommendations for a new merchant (city, category, timing, installments)

## Notes

- Uses PySpark DataFrame API only, no spark.sql()
- Nulls in `category` are replaced with "Unknown category" per spec
- Merchants not found in merchants.csv are identified by their merchant_id
- Tested locally on macOS with local[*] Spark mode
