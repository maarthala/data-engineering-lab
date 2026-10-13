# pandas — the data engineer's Swiss army knife

## Concept
**pandas** is Python's in-memory table library. Its **DataFrame** is a spreadsheet-in-code:
named, typed columns you can filter, transform, group, and join — the same operations you did in
SQL, now in Python. It's the tool you reach for to inspect data, prototype transforms, clean a
file, or reshape an API response before loading it.

!!! note "pandas vs Spark — when to use which"
    pandas runs **on one machine, in memory** — perfect up to ~a few million rows. When data is
    too big for one machine, you use **Spark** ([Unit 4](../unit4/fundamentals.md)), whose
    DataFrame API is deliberately pandas-like. Learn the shapes here; they carry over.

### What a DataFrame is

Before any code, get the two core objects clear in your head — everything else in this lesson is
just a method you call on one of them:

- A **DataFrame** is a **table held in memory**: it has **rows** and **named, typed columns**.
  Think of a spreadsheet or a SQL table that lives inside your Python program. Each column has a
  single **dtype** (data type) — `int64`, `float64`, `object` (text), `bool`, `datetime64`, and so
  on — so pandas knows how to store and operate on it fast.
- A **Series** is a **single column** — one named, typed array of values plus an **index** (the
  row labels). When you pull one column out of a DataFrame, you get a Series. A DataFrame is
  essentially a dict of Series that all share the same index.

!!! info "The index — the row labels"
    Every DataFrame and Series carries an **index**: the labels down the left edge. By default it's
    just the row numbers `0, 1, 2, …`, but it can be any labels. The index is what `loc` uses to
    look rows up, and it's what pandas aligns on when you combine objects. For now: default
    integer index = "row number."

The verbs you already know from SQL all have a pandas twin. Keep this map in mind as you go:

| SQL | pandas |
|---|---|
| `SELECT col` | `df["col"]` |
| `WHERE x > 0` | `df[df["x"] > 0]` |
| `GROUP BY … ` | `df.groupby(…).agg(…)` |
| `JOIN` | `df.merge(other, on=…, how=…)` |
| `ORDER BY` | `df.sort_values(…)` |

First, import the library. The alias **`pd`** is a universal convention — every pandas tutorial,
answer, and codebase uses it, so `pd.something` reads the same everywhere:

```python
import pandas as pd
```

**Read it step by step:**

- **`import pandas`** — load the library.
- **`as pd`** — bind it to the short name `pd`. From now on, `pd.DataFrame`, `pd.cut`, etc.

## Lab

### Create & inspect
A DataFrame is columns of data. Build one, then look at it — **inspection is the first thing you
always do** with any new dataset, so you know its shape, its columns' types, and whether it has
gaps before you trust it.

Here we build a DataFrame **from a dict**: each key becomes a **column name**, and its list of
values becomes that column's cells. All the lists must be the same length — one value per row.
(In real work you'd instead **read** a DataFrame from a file or a database — see the note below —
but a hand-built one is perfect for learning the verbs.)

```python
df = pd.DataFrame({
    "order_id":  [1, 2, 3, 4, 5],
    "country":   ["US", "UK", "US", "DE", "UK"],
    "amount":    [120.0, 40.0, 300.0, 90.0, 55.0],
    "status":    ["delivered", "cancelled", "delivered", "delivered", "delivered"],
})

df.head()          # first rows
df.shape           # (rows, cols) → (5, 4)
df.info()          # columns, dtypes, non-null counts
df.describe()      # summary stats for numeric columns
df.dtypes          # the type of each column
```

**Read it step by step:**

- **`pd.DataFrame({...})`** — build a table. The dict's **keys** (`"order_id"`, `"country"`, …)
  become the four **column names**; each **value list** becomes that column, top to bottom. Result:
  a **5-row × 4-column** DataFrame with a default index `0…4`.
- **`df.head()`** — show the **first rows** (5 by default; pass a number for more/fewer). Your
  go-to "let me eyeball it" command. Its twin `df.tail()` shows the *last* rows.
- **`df.shape`** — the DataFrame's dimensions as a tuple **`(rows, columns)`** → here `(5, 4)`. Not
  a method call, just an attribute (no parentheses).
- **`df.info()`** — a per-column summary: the column names, each one's **dtype**, and its
  **non-null count** (how many cells are filled). The quickest way to spot missing data and wrong
  types.
- **`df.describe()`** — summary **statistics** (count, mean, std, min, quartiles, max) for the
  **numeric** columns only. Great for a fast sanity check on ranges and outliers.
- **`df.dtypes`** — the **type of each column**. Here `order_id` is `int64`, `amount` is
  `float64`, and the text columns are `object`. An attribute, like `shape`.

!!! info "In real life you *read* a DataFrame, you don't type it"
    You'll rarely hand-build a DataFrame. Instead you load one with a `read_*` function, and pandas
    infers the columns and dtypes for you:

    - **`pd.read_csv("orders.csv")`** — read a CSV file into a DataFrame.
    - **`pd.read_parquet("orders.parquet")`** — read a **Parquet** file (columnar, typed, compressed —
      the format you'll meet in [Unit 4](../unit4/fundamentals.md)). Faster and safer than CSV
      because the types are stored in the file.
    - **`pd.read_sql("SELECT * FROM orders", conn)`** — run SQL against a database connection and
      pull the result straight into a DataFrame.

    Whatever the source, once it's a DataFrame the rest of this lesson is identical.

### Select columns & rows
Picking out pieces of a DataFrame is the pandas equivalent of `SELECT`. There are two axes:
choosing **columns** (with `[]`) and choosing **rows** (with `loc`/`iloc`). Watch the bracket
count — **one** name gives you a Series; a **list** of names gives you a DataFrame.

```python
df["amount"]                      # one column (a Series)
df[["country", "amount"]]         # several columns (a DataFrame)

df.loc[0]                         # a row by label
df.iloc[0:2]                      # rows by position (first two)
df.loc[df["amount"] > 100]        # boolean filter — the pandas WHERE
```

**Read it step by step:**

- **`df["amount"]`** — single square brackets with **one** column name → returns that column as a
  **Series** (a 1-D labelled array). This is `SELECT amount`.
- **`df[["country", "amount"]]`** — **double** brackets = a **list** of columns → returns a
  **DataFrame** with just those columns, in that order. This is `SELECT country, amount`. Remember
  the rule: **list in → DataFrame out; name in → Series out.**
- **`df.loc[0]`** — **`loc`** selects by **label**. Here it grabs the row whose index label is `0`,
  returned as a Series (the row's values, indexed by column name).
- **`df.iloc[0:2]`** — **`iloc`** selects by **integer position**. `0:2` means positions 0 and 1
  (the end is *excluded*, like normal Python slicing) → the first two rows as a DataFrame.
- **`df.loc[df["amount"] > 100]`** — you can pass `loc` a **boolean mask** (see the next block) to
  keep only the rows where the condition is `True`. This is the pandas `WHERE`.

!!! note "`loc` vs `iloc` — label vs position"
    - **`loc`** works with the **index labels** and the **column names** (`df.loc[0, "amount"]`).
      With the default integer index the label happens to equal the position, which is why they
      look alike at first — but reorder or filter the rows and the labels stay attached to their
      data while positions renumber.
    - **`iloc`** works with **integer positions** only (`df.iloc[0, 2]` = row 0, column 2), always
      counting from 0, always end-exclusive.

### Filter (boolean indexing)
**Boolean indexing** is how pandas does `WHERE`, and it's worth understanding the two-step trick
underneath. A comparison like `df["status"] == "delivered"` doesn't return rows — it returns a
**Series of `True`/`False`**, one per row (the **mask**). Putting that mask inside `df[...]` then
keeps only the rows where it's `True`.

```python
delivered = df[df["status"] == "delivered"]
big_us    = df[(df["country"] == "US") & (df["amount"] > 100)]   # & = and, | = or
```

**Read it step by step:**

- **`df["status"] == "delivered"`** — build a boolean mask: `True` for each delivered row, `False`
  otherwise. **`df[...]`** around it keeps the `True` rows → `delivered` is the delivered orders.
- **`(df["country"] == "US") & (df["amount"] > 100)`** — two masks combined with **`&`** (element-wise
  **and**). Both must be `True` for a row to survive → US orders over 100.
- Result grain is unchanged — still one row per order — you just have **fewer rows**.

!!! warning "Use `&` `|` `~` and wrap each condition in parentheses"
    Inside a mask you must use **`&`** (and), **`|`** (or), **`~`** (not) — *not* Python's `and`/
    `or`/`not`, which only work on single `True`/`False` values, not whole Series. And because `&`
    binds tighter than `==`, **each condition needs its own parentheses**:
    `(df["country"] == "US") & (df["amount"] > 100)`. Forget them and you'll get a confusing error.

### More filters: `isin`, `between`, `str.contains`, `query()` — and `.filter()`
Writing every condition as `==` / `>` gets clumsy fast. pandas has shortcuts that each **build a
mask** for you, plus `query()`, which lets you write the condition as a SQL-like string.

```python
df[df["country"].isin(["UK", "DE"])]           # IN ('UK', 'DE')
df[df["amount"].between(50, 150)]              # BETWEEN 50 AND 150
df[~df["status"].str.contains("cancel")]       # NOT LIKE '%cancel%'
df.query('country == "US" and amount > 100')   # the whole WHERE as one string
```

**Read it step by step:**

- **`df["country"].isin(["UK", "DE"])`** — **`isin(list)`** is `True` when the value is **one of**
  the list's items — the pandas `IN (...)`. Output: rows 1, 3, 4 (UK, DE, UK).

    ```
       order_id country  amount     status
    1         2      UK    40.0  cancelled
    3         4      DE    90.0  delivered
    4         5      UK    55.0  delivered
    ```

- **`df["amount"].between(50, 150)`** — **`between(low, high)`** is `True` when
  `low <= value <= high` — **both ends included**, exactly like SQL `BETWEEN`. Output: orders 1, 4, 5
  (120, 90, 55); 40 and 300 fall outside.
- **`~df["status"].str.contains("cancel")`** — **`.str`** unlocks **text functions** on a column of
  strings; **`.str.contains("cancel")`** is `True` when the text **contains** that piece (like
  `LIKE '%cancel%'`). The **`~`** flips the mask (**not**) → every order **except** the cancelled
  one: orders 1, 3, 4, 5.
- **`df.query('country == "US" and amount > 100')`** — **`query()`** takes the condition as a
  **string** and uses the column names directly — no `df[...]` repeated, and inside the string you
  **may** write plain `and` / `or` / `not`. Same result as `big_us` above:

    ```
       order_id country  amount     status
    0         1      US   120.0  delivered
    2         3      US   300.0  delivered
    ```

!!! tip "Use a Python variable in `query()` with `@`"
    Inside the string, a bare name means a **column**. Put **`@`** in front to mean a **Python
    variable** instead:

    ```python
    min_amt = 80
    df.query("amount >= @min_amt")      # orders 1, 3, 4 (120, 300, 90)
    ```

**`.filter()` — careful, it picks *columns*, not rows.** If you come from Spark, where
`df.filter(...)` is the `WHERE`, this one surprises everyone: pandas' **`.filter()`** keeps the
**columns** (labels) whose **names** match, and never looks at the data.

```python
df.filter(items=["order_id", "amount"])   # exactly these columns
df.filter(like="ount")                    # names CONTAINING "ount"
df.filter(regex="^(c|s)")                 # names matching a regular expression
```

- **`items=[...]`** — keep exactly the listed columns → `order_id`, `amount` (same as
  `df[["order_id", "amount"]]`).
- **`like="ount"`** — keep columns whose **name contains** `"ount"` → `country`, `amount`.
- **`regex="^(c|s)"`** — keep columns whose name matches a **regular expression** (a text pattern):
  **`^`** = "starts with", **`(c|s)`** = "`c` or `s`" → `country`, `status`.

To filter rows **and** pick columns, chain the two — mask first, then `.filter()`:

```python
df[df["amount"] > 100].filter(items=["order_id", "amount"])
```

```
   order_id  amount
0         1   120.0
2         3   300.0
```

!!! note "Spark `.filter()` vs pandas `.filter()`"
    | You want | Spark | pandas |
    |---|---|---|
    | Keep **rows** (`WHERE`) | `df.filter(col("amount") > 100)` | `df[df["amount"] > 100]` or `df.query("amount > 100")` |
    | Keep **columns** (`SELECT`) | `df.select("order_id", "amount")` | `df[["order_id", "amount"]]` or `df.filter(items=[...])` |

### Add & transform columns
New columns are computed from existing ones. The key idea is **vectorisation**: an operation like
`df["amount"] * 1.2` runs on the **whole column at once**, not row by row — it's both shorter to
write and far faster than a Python loop.

```python
df["with_tax"]  = df["amount"] * 1.2                 # vectorised arithmetic
df["band"]      = pd.cut(df["amount"], [0, 100, 1000],
                         labels=["small", "large"])   # bucketing
df = df.assign(is_delivered = df["status"] == "delivered")   # assign() returns a new df
```

**Read it step by step:**

- **`df["with_tax"] = df["amount"] * 1.2`** — assigning to a **new column name** creates it.
  The right side multiplies every value in `amount` by 1.2 in one shot (**vectorised arithmetic**).
- **`pd.cut(df["amount"], [0, 100, 1000], labels=[...])`** — **bucket** a numeric column into
  bins. The list `[0, 100, 1000]` defines the bin edges (0–100 and 100–1000); `labels` names them.
  Each row gets the label of the bin its `amount` falls into → a categorical `band` column.
- **`df.assign(is_delivered = ...)`** — **`assign`** adds one or more columns and **returns a new
  DataFrame** (it doesn't modify `df` in place). That's why we catch it with `df = ...`. It's handy
  in method chains because it returns the whole DataFrame to keep chaining on.

!!! note "Two ways to add a column — in place vs. a new frame"
    - **`df["new"] = ...`** mutates `df` directly (in place).
    - **`df.assign(new = ...)`** leaves `df` untouched and hands back a **copy** with the column
      added. Prefer `assign` when you want to keep the original intact or chain several steps.

### Group & aggregate (the pandas GROUP BY)
This is the pandas twin of SQL's `GROUP BY` + `SUM/COUNT/AVG`. **`groupby("country")`** splits the
rows into one bucket per country; **`.agg(...)`** then collapses each bucket into a single summary
row. Read the chain top to bottom — each line is one step.

```python
# revenue & order count per country, delivered only
(df[df["status"] == "delivered"]
   .groupby("country")
   .agg(revenue=("amount", "sum"),
        orders=("order_id", "count"),
        avg_order=("amount", "mean"))
   .reset_index()
   .sort_values("revenue", ascending=False))
```

**Read it step by step:**

- **`df[df["status"] == "delivered"]`** — first filter to delivered orders (the `WHERE`, applied
  **before** grouping — same order as SQL).
- **`.groupby("country")`** — bucket the remaining rows by `country`. Nothing is computed yet;
  you've just declared the grouping key.
- **`.agg(revenue=("amount", "sum"), ...)`** — compute one value per bucket. This **named-aggregation**
  form reads `output_name=(source_column, function)`: `revenue` = **sum** of `amount`, `orders` =
  **count** of `order_id`, `avg_order` = **mean** of `amount`. The result has **one row per country**
  — that's the new grain.
- **`.reset_index()`** — after `groupby`, the grouping key (`country`) becomes the **index**.
  `reset_index()` pushes it back to being an ordinary column, so the result is a plain
  0,1,2-indexed DataFrame — usually what you want for further work or export.
- **`.sort_values("revenue", ascending=False)`** — order the summary rows by `revenue`, highest
  first (the `ORDER BY … DESC`).

!!! info "Reading a method chain"
    Wrapping the whole thing in `(...)` lets you put each `.method()` on its own line so the
    pipeline reads like a recipe: **filter → group → aggregate → tidy the index → sort**. Each step
    takes the DataFrame from the step above and passes its result to the next.

### Sort, rename, drop, cast
A grab-bag of everyday reshaping verbs. Note that `sort_values`, `rename`, and `drop` all **return
a new DataFrame** and leave `df` unchanged unless you reassign the result — only the explicit
`df[...] = ...` line below actually mutates `df`.

```python
df.sort_values("amount", ascending=False)
df.rename(columns={"amount": "gross_amount"})
df.drop(columns=["with_tax"])
df["order_id"] = df["order_id"].astype("int64")
df["status"].value_counts()        # counts per distinct value — super handy
```

**Read it step by step:**

- **`df.sort_values("amount", ascending=False)`** — return the rows ordered by `amount`, largest
  first. Drop `ascending=False` (defaults to `True`) for smallest-first. Pass a list of columns to
  sort by several keys.
- **`df.rename(columns={"amount": "gross_amount"})`** — rename columns via an
  `{old: new}` mapping. Only the listed columns change.
- **`df.drop(columns=["with_tax"])`** — remove one or more columns by name.
- **`df["order_id"].astype("int64")`** — **cast** a column to another dtype (here to 64-bit
  integer). Common for fixing columns that loaded as the wrong type. Reassigning it back to
  `df["order_id"]` makes the change stick.
- **`df["status"].value_counts()`** — count how many rows have each **distinct value** of a column,
  sorted most-frequent first. It's `GROUP BY status → COUNT(*)` in a single call — a fast way to
  see the distribution of a categorical column.

### Transpose — flip rows and columns
**Transposing** swaps the two axes: every **row becomes a column** and every **column becomes a
row**. It's mostly a **reading aid** — a wide table (many columns, few rows) is much easier to scan
when it's turned on its side.

```python
small = df[["order_id", "country", "amount"]].head(3)
small.T                     # .T = transpose (same as small.transpose())
```

**Read it step by step:**

- **`df[["order_id", "country", "amount"]].head(3)`** — a small 3-row × 3-column piece to flip (so
  the output stays readable):

    ```
       order_id country  amount
    0         1      US   120.0
    1         2      UK    40.0
    2         3      US   300.0
    ```

- **`small.T`** — **`.T`** is the **transpose** (an attribute, no parentheses; `small.transpose()`
  is the same thing as a method). The old **column names** become the **index** (row labels), and
  the old **index** `0, 1, 2` becomes the **column names**:

    ```
                  0     1      2
    order_id      1     2      3
    country      US    UK     US
    amount    120.0  40.0  300.0
    ```

Make the columns meaningful by setting a real key as the index **first** — then those keys become
the column headers after the flip:

```python
small.set_index("order_id").T
```

```
order_id      1     2      3
country      US    UK     US
amount    120.0  40.0  300.0
```

- **`set_index("order_id")`** — move the `order_id` column into the **index** (the row labels),
  so after `.T` the columns are named `1, 2, 3` by order id instead of by position.

**Where it really helps — summaries.** `describe()` gives one column per data column and one row
per statistic; with many columns that's hard to read. `.T` turns it into **one row per
column**:

```python
df[["amount"]].describe().T
```

```
        count   mean         std   min   25%   50%    75%    max
amount    5.0  121.0  104.785495  40.0  55.0  90.0  120.0  300.0
```

And to lay a `groupby` result out **horizontally**, one column per group:

```python
df.groupby("country")["amount"].sum().to_frame().T
```

```
country    DE    UK     US
amount   90.0  95.0  420.0
```

- **`df.groupby("country")["amount"].sum()`** — total `amount` per country, as a **Series**
  (index = country).
- **`.to_frame()`** — turn that Series into a one-column **DataFrame** (a Series has only one axis,
  so there's nothing to flip until it's a DataFrame).
- **`.T`** — flip it: the countries become the **columns**, `amount` the single row.

!!! warning "Transposing mixed types turns everything into `object`"
    Each pandas column has **one** dtype. After a flip, a column holds one value **from every old
    column** — here a number, a country code and an amount — so pandas falls back to the generic
    **`object`** type:

    ```python
    small.T.dtypes      # 0 object / 1 object / 2 object
    ```

    Fine for **looking at** data; don't do maths on a transposed mixed table. Transpose to read,
    keep the original for work.

!!! note "Transpose vs pivot"
    **`.T`** flips the **whole** table, labels and all — no grouping, no aggregation. If you want
    to turn the **values** of a column into new columns (e.g. one column per month with revenue in
    the cells), that's a **pivot** — next section.

### Pivot — turn values into columns (and `melt` back)
A **pivot** takes the **distinct values of one column** and makes each of them a **new column**,
filling the cells with an aggregate. It's the spreadsheet "pivot table", and the SQL
`SUM(CASE WHEN status = 'delivered' THEN amount END) …` written for you. Here: revenue per country,
split by status.

```python
p = df.pivot_table(index="country", columns="status",
                   values="amount", aggfunc="sum", fill_value=0)
p
```

```
status   cancelled  delivered
country
DE             0.0       90.0
UK            40.0       55.0
US             0.0      420.0
```

**Read it step by step:**

- **`index="country"`** — what becomes the **rows**: one row per distinct country (the `GROUP BY`).
- **`columns="status"`** — whose **values** become the **new columns**: `cancelled`, `delivered`.
- **`values="amount"`** — which column fills the cells.
- **`aggfunc="sum"`** — how to combine several rows that land in the same cell. US has two
  delivered orders (120 + 300), so its cell is **420**. Other choices: `"mean"`, `"count"`,
  `"max"`, … (the default is `"mean"`).
- **`fill_value=0`** — what to put where **no row exists** (DE and US have no cancelled orders).
  Without it those cells would be `NaN` (missing).
- The labels **`status`** (above the columns) and **`country`** (above the index) are just the
  **names** of the two axes, so you can see where each came from.

**Add totals with `margins`** — an extra row and column with the grand totals:

```python
df.pivot_table(index="country", columns="status", values="amount",
               aggfunc="sum", fill_value=0, margins=True, margins_name="Total")
```

```
status   cancelled  delivered  Total
country
DE             0.0       90.0   90.0
UK            40.0       55.0   95.0
US             0.0      420.0  420.0
Total         40.0      565.0  605.0
```

- **`margins=True`** — add the totals row and column; **`margins_name="Total"`** labels them (the
  default label is `All`).

**Count instead of sum** — how many orders per country and status:

```python
df.pivot_table(index="country", columns="status", values="order_id",
               aggfunc="count", fill_value=0)
```

```
status   cancelled  delivered
country
DE               0          1
UK               1          1
US               0          2
```

!!! warning "`pivot()` vs `pivot_table()`"
    There is also a plain **`df.pivot(index=..., columns=..., values=...)`**. It **only reshapes —
    it never aggregates**, so every `(index, column)` pair must appear **once**. Our data has two
    `US / delivered` rows, so it fails:

    ```
    ValueError: Index contains duplicate entries, cannot reshape
    ```

    Rule of thumb: use **`pivot_table()`** — it works whether or not there are duplicates.

**`melt` — the reverse (unpivot).** Pivoting makes data **wide** (one column per status). Most
tools — Spark, SQL tables, charts — prefer it **long** (one row per country *and* status).
**`melt()`** turns the columns back into rows:

```python
wide = p.reset_index()          # country back to an ordinary column
wide.columns.name = None        # drop the leftover "status" axis label
long = wide.melt(id_vars="country", var_name="status", value_name="amount")
long
```

```
  country     status  amount
0      DE  cancelled     0.0
1      UK  cancelled    40.0
2      US  cancelled     0.0
3      DE  delivered    90.0
4      UK  delivered    55.0
5      US  delivered   420.0
```

- **`p.reset_index()`** — move `country` out of the index into a normal column (`melt` works on
  columns).
- **`wide.columns.name = None`** — remove the `status` **axis name** the pivot left above the
  columns, so it doesn't clash with the new `status` column we're about to create.
- **`melt(id_vars="country", ...)`** — **`id_vars`** = the column(s) to **keep as-is**; every
  **other** column (`cancelled`, `delivered`) is unfolded into rows.
- **`var_name="status"`** — name of the new column holding the **old column names**;
  **`value_name="amount"`** — name of the new column holding their **values**.

!!! note "Transpose vs pivot vs melt"
    | Tool | What it does |
    |---|---|
    | **`.T`** | Flip the **whole** table — rows ↔ columns. No grouping. |
    | **`pivot_table()`** | **Long → wide**: a column's **values** become new columns, cells **aggregated**. |
    | **`melt()`** | **Wide → long**: columns become rows (unpivot). |

    In Spark the same ideas are `df.groupBy("country").pivot("status").sum("amount")` and
    `df.unpivot(...)`; in SQL, `PIVOT` / `UNPIVOT` (or `CASE WHEN`).

### Handle missing data
Real data has gaps. pandas represents a missing value as **`NaN`** / `None` (its `NULL`), and gives
you three tools: **detect** them (`isna`), **drop** the affected rows (`dropna`), or **fill** them
with a default (`fillna`).

```python
df2 = df.copy()
df2.loc[1, "amount"] = None
df2["amount"].isna().sum()                 # how many nulls
df2 = df2.dropna(subset=["amount"])        # drop rows missing amount
df2 = df2.fillna({"country": "unknown"})   # or fill a default
```

**Read it step by step:**

- **`df.copy()`** — make an independent copy so edits to `df2` don't touch the original `df`.
  (Without `.copy()`, `df2` could be a view onto `df` and changes might leak back.)
- **`df2.loc[1, "amount"] = None`** — use `loc[row_label, column]` to set a single cell. Here we
  poke a null into row `1`'s `amount` to have something to clean.
- **`df2["amount"].isna().sum()`** — **`isna()`** returns a boolean mask (`True` where the value is
  missing); summing a boolean mask counts the `True`s → **how many nulls** are in the column.
- **`df2.dropna(subset=["amount"])`** — **drop rows** that have a missing value in the listed
  columns. Without `subset` it drops rows with a null in *any* column.
- **`df2.fillna({"country": "unknown"})`** — **fill** missing values. The dict form fills
  per-column: nulls in `country` become `"unknown"`. Both `dropna` and `fillna` return a new
  DataFrame, hence the `df2 = ...` reassignment.

### Join two DataFrames
**`merge`** is the pandas `JOIN`: it stitches two DataFrames together on a shared **key** column,
exactly like the joins you did in [Unit 2](../unit2/joins-aggregations.md). Here we attach a
`region` to each order by matching on `country`.

```python
countries = pd.DataFrame({"country": ["US", "UK", "DE"],
                          "region":  ["NA", "EU", "EU"]})
enriched = df.merge(countries, on="country", how="left")   # SQL LEFT JOIN
```

**Read it step by step:**

- **`countries = pd.DataFrame({...})`** — a small lookup table mapping each country to a region.
- **`df.merge(countries, ...)`** — join `df` (left) to `countries` (right).
- **`on="country"`** — the **join key**: rows match where their `country` values are equal (the
  pandas `ON o.country = c.country`). The `region` column comes along onto each matched order row.
- **`how="left"`** — a **LEFT JOIN**: keep **every** row of `df`; if a country has no match in
  `countries`, its `region` comes back as `NaN`. Other options are `"inner"` (only matches),
  `"right"`, and `"outer"` (all rows from both) — identical to SQL join types.
- Result: `enriched` has all of `df`'s rows plus the new `region` column.

!!! note "`merge` mirrors SQL joins one-for-one"
    `df.merge(other, on="key", how="left")` **is** `SELECT * FROM df LEFT JOIN other USING(key)`.
    If the key columns have **different names**, use `left_on=` / `right_on=` instead of `on=`.
    Just like SQL, if the key isn't unique on the right side the join **fans out** to more rows —
    watch for that.

### `apply` / `map` — custom row logic
When there's no built-in, apply a Python function (slower — prefer vectorised ops when you can):

```python
df["band2"] = df["amount"].apply(lambda a: "large" if a >= 100 else "small")
```

**Read it step by step:**

- **`df["amount"].apply(func)`** — **`apply`** runs the given function on **each value** of the
  column and collects the results into a new Series.
- **`lambda a: "large" if a >= 100 else "small"`** — an inline one-argument function: `a` is one
  `amount` value; it returns `"large"` or `"small"`. So each row is labelled by its own amount.
- Assigning the result to `df["band2"]` stores it as a new column.

!!! warning "`apply` is your last resort, not your first"
    `apply` calls your Python function once per row, so it's **much slower** than a vectorised
    expression on the whole column. Reach for a built-in / vectorised form first
    (`df["amount"] * 1.2`, `pd.cut(...)`, boolean masks) and use `apply` only when the logic
    genuinely can't be expressed that way.

## Challenge
Build a **region scorecard**. This ties the lesson together — a **join** *and* a group-by:

1. `merge` the `region` lookup (from *Join two DataFrames* above) onto the orders.
2. Keep **delivered** orders only.
3. Per **`region`** (not country), compute total revenue, order count, and average order value.
4. Sort by revenue, highest first.

The twist vs. the *Group & aggregate* example: you group by the **joined-in `region`**, so you
first have to `merge`, then group.

??? note "Solution"
    ```python
    countries = pd.DataFrame({"country": ["US", "UK", "DE"],
                              "region":  ["NA", "EU", "EU"]})
    enriched  = df.merge(countries, on="country", how="left")   # attach region (LEFT JOIN)

    region_scorecard = (
        enriched[enriched["status"] == "delivered"]             # delivered only
            .groupby("region")                                  # ← the joined-in key
            .agg(revenue=("amount", "sum"),
                 orders=("order_id", "count"),
                 avg_order=("amount", "mean"))
            .reset_index()
            .sort_values("revenue", ascending=False)
    )
    region_scorecard
    ```

    Expected result (from the 5-row sample `df`):

    ```text
    region  revenue  orders  avg_order
        NA    420.0       2      210.0
        EU    145.0       2       72.5
    ```

!!! tip "🎯 The same shapes on Azure, Databricks, Snowflake & Fabric"
    **What you just did:** filtered, transformed, grouped, and joined DataFrames.

    - **Azure Databricks / Microsoft Fabric** — run pandas directly in notebooks; and Spark's
      DataFrame API (`.filter/.groupBy/.join`) mirrors these shapes at scale — plus
      **pandas API on Spark** (`import pyspark.pandas as ps`) is pandas that runs distributed.
    - **Snowflake** — **Snowpark**'s DataFrame API mirrors pandas; `to_pandas()` bridges them.
    - **Azure Data Factory** — the no-code Mapping Data Flow expresses the same filter/derive/
      aggregate/join steps as visual transformations.

    pandas is the universal prototyping tool; these verbs reappear in every DataFrame API.

## Key terms, at a glance
| Term | Plain meaning |
|---|---|
| **DataFrame** | A table in memory: rows + named, typed columns |
| **Series** | A single column — one named, typed array with an index |
| **Index** | The row labels down the left edge (default `0,1,2,…`) |
| **dtype** | A column's type (`int64`, `float64`, `object`, `bool`…) |
| **`read_csv` / `read_parquet` / `read_sql`** | Load a DataFrame from a file / Parquet / a database |
| **`head` / `shape` / `info` / `describe` / `dtypes`** | The inspection toolkit: first rows / (rows,cols) / column summary / stats / types |
| **`df['col']` vs `df[['a','b']]`** | One name → a Series; a list → a DataFrame |
| **Boolean indexing** | `df[df.col > x]` — mask of True/False → the pandas WHERE |
| **`&` `\|` `~`** | Element-wise and / or / not inside a mask (wrap each condition in `()`) |
| **`loc` / `iloc`** | Select by label / by integer position |
| **Vectorised op** | An operation on a whole column at once (fast) vs a Python loop |
| **`assign`** | Return a new DataFrame with column(s) added |
| **`pd.cut`** | Bucket a numeric column into labelled bins |
| **`groupby().agg()`** | Group + aggregate (the GROUP BY); `reset_index()` un-nests the key |
| **`merge`** | Join two DataFrames on a key (`how='left'/'inner'/…`) = SQL JOIN |
| **`sort_values`** | Order rows by one or more columns (the ORDER BY) |
| **`rename` / `drop` / `astype`** | Rename columns / drop columns / cast a column's type |
| **`value_counts`** | Count rows per distinct value (GROUP BY + COUNT in one) |
| **`isna` / `dropna` / `fillna`** | Detect / drop / fill missing values (`NaN`) |
| **`apply` / `map`** | Run a custom Python function per value (last resort — slower) |
| **Grain** | What one output row represents (per order, per country…) |

## You can now…
- Create and **inspect** DataFrames (`head/info/describe/dtypes/shape`)
- Select, filter, add, and transform columns
- Group & aggregate, sort, join, and handle missing data
- Recognise how every pandas verb maps to SQL and to Spark
