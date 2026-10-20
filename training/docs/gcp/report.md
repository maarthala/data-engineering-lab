# A small report in Looker Studio

## Concept

**Looker Studio** (`lookerstudio.google.com`) is Google's free report and dashboard tool, the
role Superset plays in the lab. A report has **data sources** (here two Gold tables in
BigQuery) and **charts** on pages. Each chart picks:

- **Dimensions**: what to group by (`order_date`, `channel`, `category`)
- **Metrics**: what to add up (`revenue`, `orders`)

Looker Studio turns every chart into a BigQuery query, so the work runs in BigQuery. Gold tables
are small and already summed up, which keeps those queries cheap and quick. That's why reports
read Gold, not Silver.

!!! info "Looker Studio vs Looker"
    **Looker** (without *Studio*) is a separate, paid BI platform with a modelling language
    (LookML). This lesson uses the free **Looker Studio**.

## Lab

### 1 · Connect the Gold tables
1. Open `lookerstudio.google.com` with the same Google account → **Create** → **Report**.
2. **Add data to report** → **BigQuery** → **My projects** → `shopflow-de` → `shopflow_gold` →
   `daily_sales` → **Add** → **Add to report**.
3. Looker Studio drops a table on the page. Delete it (select it, **Delete**).
4. **Add data** (toolbar) → BigQuery → `shopflow_gold` → `top_products` → **Add**.
5. Rename the report (top left) to **ShopFlow sales**.

### 2 · Three scorecards
**Add a chart** → **Scorecard**. In the panel on the right (**Setup**):

| Scorecard | Data source | Metric |
|---|---|---|
| Revenue | `daily_sales` | `revenue` (SUM) |
| Delivered orders | `daily_sales` | `orders` (SUM) |
| Units | `daily_sales` | `units` (SUM) |

- **SUM** is the **aggregation**: how a metric adds up over the rows in the chart. Click the
  metric to change it (AVG, MAX, …).

Expected values (whole period):

| Revenue | Delivered orders | Units |
|---|---|---|
| 53.6M | 28,005 | 209.5K |

If you ran the new day through Composer, it's **54.7M**, **28,576** orders and **213.8K** units.

### 3 · Revenue over time
**Add a chart** → **Time series**:

- **Dimension**: `order_date`. Click the calendar icon next to it → **Year Month**, so you get
  one point per month instead of one per day.
- **Metric**: `revenue`
- **Breakdown dimension**: `channel`, for one line per channel.

The `web` line sits on top: web brings about 43 % of revenue (23.1M of 53.6M).

### 4 · Revenue by category
**Add a chart** → **Bar chart**, data source **`top_products`**:

- **Dimension**: `category`, **Metric**: `revenue`, sorted by `revenue` descending.

| category | revenue |
|---|---|
| Home & Kitchen | 8.5M |
| Furniture | 7.9M |
| Books | 7.6M |
| Clothing | 7.3M |
| Stationery | 7.3M |
| Electronics | 5.2M |
| Toys | 5.1M |
| Sports | 4.8M |

`top_products` has one row per product. The bar chart **groups** by `category` and SUMs the
revenue for you.

### 5 · Top 10 products
**Add a chart** → **Table**, data source `top_products`:

- **Dimensions**: `name`, `category`. **Metrics**: `revenue`, `margin`, `quantity`.
- **Sort**: `revenue` descending. **Rows per page**: `10`.

Product 173 (Home & Kitchen) is first, with 564,400 revenue.

### 6 · Let the reader filter
- **Add a control** → **Date range control**: put it top right. It filters every chart from
  `daily_sales` to the dates picked.
- **Add a control** → **Drop-down list**, **Control field** `channel`: the reader can pick
  `web`, `app`, … and the charts on that data source follow.

Click **View** (top right) to see the report the way a reader does, then try the controls.

### 7 · Share it
**Share** → add a person's e-mail as **Viewer**.

- By default a report uses the **owner's credentials**: viewers see the data through *you*,
  without needing access to BigQuery. You can change that per data source (**Resource → Manage
  added data sources → Edit → Data credentials**).
- The queries run in **your** project. For Gold tables this small, that's next to nothing.

## Check yourself
1. Why does the report read Gold and not Silver? *(Gold is small and already summed, so every
   chart's query is cheap and fast. Silver would make each chart re-add 39,900 orders.)*
2. In the time series, what turns days into months? *(Setting the date dimension's type to
   **Year Month**.)*
3. A viewer has no BigQuery access. Can they see the report? *(Yes, with the default owner's
   credentials. The data is queried as the report's owner.)*
