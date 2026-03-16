# Billups Data Engineering Challenge — Report

## Dataset Overview

- **Transactions:** 7,274,367 records (Jan 2017 – Feb 2018)
- **Merchants:** deduplicated to one record per merchant_id (kept best `most_recent_sales_range`)
- **Join:** left join on `merchant_id`, 1 unmatched merchant uses its ID as name

---

## Question 1: Top 5 Merchants by Month and City

Results show the top 5 merchants by total purchase volume for each month/city combination.
Dense rank is used so tied merchants both appear instead of one getting cut arbitrarily.

[fill in key findings after running — e.g. which merchants dominate, any city with clear winner every month]

---

## Question 2: Average Sale Amount per Merchant per State

Merchants ranked by their average ticket size per state. Gives a sense of which merchants operate
in higher-value segments and which states tend to have larger or smaller transactions.

[fill in after running — top merchants, any state patterns]

---

## Question 3: Top 3 Peak Hours per Category

For each product category, the 3 hours with the highest total sales volume.
Hour formatted as military time (0800, 1300, etc).

[fill in after running — do categories share the same peak hours or diverge?]

---

## Question 4: Popular Merchants by City and Category Correlation

Two parts:

**Part A — Popular merchants by city:** counts transactions per merchant and shows the city where
each merchant is physically located (`merchant_city_id` from merchants table, not transaction origin).
Note: `merchant_city_id = -1` means the merchant's location is unknown (~31% of merchants). These
are kept in the analysis since filtering them would remove a large chunk of data.

**Part B — City-category correlation:** shows transaction count and % share per city/category pair.
A high % for one category in a city indicates concentration (possible correlation).

[fill in after running — which cities show clear category dominance? any suprising patterns?]

---

## Question 5: Recommendations for a New Merchant

### a) Recommended Cities

Reuses the city summary from Q4. Top cities by transaction volume and number of active merchants.

[fill in top 3-5 cities with reasoning]

### b) Recommended Categories

Categories ranked by total revenue (Unknown category excluded). Helps pick what to sell.

[fill in after running — category A vs B, is the avg ticket difference meaningful?]

### c) Seasonal Patterns

Monthly totals from Jan 2017 to Feb 2018. Look for spikes around holidays or end of year.

[fill in after running — any months with noticeably higher volume or avg ticket?]

### d) Recommended Hours

Hourly distribution of transactions and revenue. Recommend opening hours that cover the top
traffic window without overstretching staff.

[fill in after running — likely recommending something like 10am-8pm based on distribution]

### e) Installment Analysis

**Model assumptions (from spec):**
- Monthly default rate: 22.9% — cumulative for N installments: `1 - (1 - 0.229)^N`
- Gross margin: 25% of revenue
- Defaulters pay `floor(N/2)` installments before stopping
- Installments = 999 excluded (50 records, data error)

The cumulative default probability grows quickly with more installments:
- 2 installments → ~39.8% default risk
- 6 installments → ~79.3% default risk
- 12 installments → ~95.8% default risk

The chart below shows where installments stop being profitable:

![Effective Margin by Installments](q5e_installment_margin_chart.png)

[fill in after running — at what installment count does effective margin go negative?]

**Recommendation:** [fill in — likely: only offer installments up to N=X, beyond that expected
profit per sale turns negative due to compounding default risk]

---

## Assumptions and Data Cleaning Notes

- **Merchant duplicates:** 63 merchant_ids had multiple entries with different data. Kept the record
  with the best `most_recent_sales_range` (A > B > C > D > E) as a proxy for the most active record.
- **Null categories:** replaced with "Unknown category" (44,625 records). Not filtered — the spec
  says to keep all records.
- **Installments = 999:** excluded from installment analysis only (50 records, likely data errors).
  Kept in all other questions.
- **merchant_city_id = -1:** kept throughout. Represents merchants without a known location.
- **purchase_date:** stored as string in the parquet, parsed to timestamp for all date operations.
- **city_id / state_id:** the unqualified columns refer to the transaction location (where the
  purchase happened). The merchant's own location uses `merchant_city_id` / `merchant_state_id`.


