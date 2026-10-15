# Docs and lineage

## Concept
Three months from now someone asks: *"Where does `gold.daily_orders` come from, and what does
`status = 'placed'` mean?"* In most teams the answer is in someone's head. dbt already knows
half of it: every `ref()` and `source()` is a line in the **lineage** graph. Add a sentence per
model and column, and **`dbt docs generate`** turns the project into a **documentation
website**: every model, its columns and types, its SQL, its tests, and the graph.

Documentation lives **next to the code**, in the same YAML as the tests, so it's reviewed and
changed together with the model it describes.

An **exposure** describes something **outside** dbt that uses your models: a dashboard, a
report, an ML model. It adds the last box to the graph: *which dashboard breaks if this
model changes?*

## Lab
Continue in `~/work/my_files/dbt/shop`.

### 1 · Describe the source and the models
Add **`description:`** lines. In `models/staging/sources.yml`:

```yaml
version: 2

sources:
  - name: bronze                 # the schema the raw table lives in
    description: Raw ShopFlow data, loaded as-is into your lakehouse.
    tables:
      - name: sample_orders      # my.bronze.sample_orders — your starter table
        description: 1,000 ShopFlow orders, one row per order.
```

In `models/staging/schema.yml`, one for the model and one per column (the tests stay):

```yaml
version: 2

models:
  - name: stg_orders
    description: Clean orders, one row per order, with the timestamp cut to a date.
    columns:
      - name: order_id
        description: Order number.
        data_tests:
          - unique
          - not_null
      - name: customer_id
        description: The customer who placed the order.
        data_tests:
          - not_null
      - name: channel
        description: Where the order came from (see the channels seed).
        data_tests:
          - accepted_values:
              arguments:
                values: ['web', 'app', 'marketplace', 'wholesale']
          - relationships:
              arguments:
                to: ref('channels')
                field: channel
      - name: status
        description: '{{ doc("order_status") }}'
```

A column can have a description without tests (`status`), and the other way round.

### 2 · Longer text: a doc block
`status` needs a table, not a sentence. Write it once, as a **doc block**, in any `.md` file in
`models/`: **`models/docs.md`**:

```text
{% docs order_status %}
Where the order is in its life cycle:

| status | meaning |
|---|---|
| placed | paid, not shipped yet |
| shipped | on its way |
| delivered | arrived |
| cancelled | stopped before shipping |
{% enddocs %}
```

- **`{% docs name %} … {% enddocs %}`**: a named piece of **Markdown** (headings, lists,
  tables), the format these course pages are written in.
- **`'{{ doc("order_status") }}'`** in `schema.yml` (step 1) pulls it in. Any column in any model
  can use the same block, so the meaning of a status is written in one place.
- The four values are the real ones: the `accepted_values` idea from
  [Tests and seeds](tests.md) works for documentation too. Check with a `GROUP BY status`
  before you describe data.

### 3 · Who uses the marts: an exposure
New file **`models/marts/exposures.yml`**:

```yaml
version: 2

exposures:
  - name: sales_dashboard
    label: Sales dashboard
    type: dashboard
    owner:
      name: <you>
    description: The Superset dashboard on the gold marts.
    depends_on:
      - ref('orders_by_channel')
      - ref('daily_orders')
```

- **`type`**: `dashboard`, `notebook`, `analysis`, `ml` or `application`.
- **`owner`**: who to ask. A `name` or an `email` is required; put your username.
- **`depends_on`**: the models it reads, with `ref()`.

An exposure builds nothing. But selectors understand it: **everything the dashboard needs**:

```bash
dbt ls -s +exposure:sales_dashboard
```
```text
exposure:shop.sales_dashboard
shop.marts.daily_orders
shop.marts.orders_by_channel
shop.staging.stg_orders
source:shop.bronze.sample_orders
shop.staging.accepted_values_stg_orders_channel__web__app__marketplace__wholesale
shop.no_future_orders
shop.staging.not_null_stg_orders_customer_id
shop.staging.not_null_stg_orders_order_id
shop.staging.relationships_stg_orders_channel__channel__ref_channels_
shop.staging.unique_stg_orders_order_id
```

So `dbt build -s +exposure:sales_dashboard` refreshes exactly what the dashboard shows,
tests included.

### 4 · Build the site: `dbt docs generate --static`

```bash
dbt docs generate --static
```
```text
16:12:01  Running with dbt=1.12.5
16:12:04  Found 1 seed, 1 snapshot, 5 models, 6 data tests, 1 source, 1 exposure, 633 macros
16:12:07  Building catalog
16:12:07  Catalog written to /home/jovyan/work/dbt/shop/target/catalog.json
```

- **`dbt docs generate`** collects the project (models, YAML, SQL) and the **catalog**: the
  real columns and types, read from your lakehouse.
- **`--static`** puts everything into **one file**, `target/static_index.html` (about 2.5 MB),
  that opens in any browser with no server.

### 5 · Open it
In Jupyter's file browser, go to `my_files/dbt/shop/target/`, right-click **`static_index.html`** →
**Download**, and open the downloaded file in your browser. What's in it:

- **Left: Project tab**: your folders (sources, models, seeds, snapshots, exposures, macros);
  **Database tab**: the same objects by schema (`bronze`, `silver`, `gold`, `snapshots`).
- **A model page**: its description, its columns with types and descriptions, the tests on
  each column, **Referenced by** / **Depends on**, and its SQL (**Source** = yours, **Compiled**
  = what ran).
- **The lineage graph**: the round blue button bottom right. From `sample_orders` to
  `stg_orders` to the marts and on to `sales_dashboard`. Click a box to open it; type
  `+daily_orders` in the **--select** box at the bottom to show only that part.

!!! note "Two gaps you'll notice"
    - `stg_orders` shows **no column types**: dbt-spark reads them only for tables, not for
      views. The descriptions are there.
    - The page is a **snapshot of now**: regenerate it after you change the project. In a
      pipeline, `dbt docs generate` runs after `dbt build`.

## 🎯 This runs unchanged on Azure, Databricks, Snowflake & Fabric
Descriptions, doc blocks, exposures and `dbt docs generate` are core dbt. dbt Cloud hosts the
site for you (*dbt Explorer*). On Databricks and Snowflake, the **`persist_docs`** config also
writes your descriptions into the platform's own catalog, as table and column comments.

## You can now…
- Document sources, models and columns with **`description:`**
- Write a **doc block** once and use it with `{{ doc() }}`
- Declare an **exposure**, and select everything it depends on with `+exposure:<name>`
- Build the docs site with **`dbt docs generate --static`**, and read the lineage graph
