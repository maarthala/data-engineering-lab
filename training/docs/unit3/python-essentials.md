# Python & notebooks for data engineering

## Concept
SQL ([Unit 2](../unit2/intro.md)) answers questions once the data is in tables. But a data
engineer also needs a **general-purpose language** to glue systems together: pull from an API,
reshape a file, call a database, kick off a Spark job, add custom logic SQL can't express.
That language is **Python** — the lingua franca of data engineering — and you'll write it in a
**notebook**.

### Why notebooks?
A **Jupyter notebook** runs code in **cells** you execute one at a time, seeing the output
immediately below each. That tight feedback loop is perfect for exploring data. Open Jupyter at
[http://jupyter.lab.localhost:8000](http://jupyter.lab.localhost:8000) (sign in with your lab account — you get your own Jupyter) and make a new notebook.

- **Shift+Enter** runs a cell.
- The result of the **last expression** in a cell is printed automatically.
- Variables persist across cells (top-to-bottom is the intended order).

#### How a notebook actually runs your code
A notebook is a list of **cells** — little boxes you type code into. You press **Shift+Enter** to
run one, and Jupyter shows the output directly underneath it. Two ideas are worth holding onto
before you start, because they trip up everyone new to notebooks:

- **State persists across cells.** When you run a cell, any variable it creates *stays alive* for
  every cell that runs afterward. If cell 1 sets `orders = 40000`, cell 5 can still use `orders`
  without redefining it. The notebook remembers everything you've run, like a conversation that
  keeps its memory.
- **Run order is whatever *you* clicked, not top-to-bottom.** The notebook doesn't automatically
  run cells in the order they appear on screen — it runs them in the order you press Shift+Enter.
  That's why the intended order is top-to-bottom: if you run a cell out of order (or edit a cell
  above and forget to re-run it), you can get confusing results. When in doubt, use the menu's
  *Restart & Run All* to run everything cleanly from the top.

The last line below is an **expression** — a piece of code that computes a value. Jupyter
automatically prints the value of the **last expression** in a cell (you don't need `print`):

```python
1 + 1          # a cell whose last expression is shown → 2
```

**Read it step by step:**

- **`1 + 1`** — ordinary arithmetic; Python evaluates it to `2`.
- **`# a cell whose last expression is shown → 2`** — everything after a **`#`** is a **comment**.
  Python ignores comments entirely; they're notes for humans. You'll see them all over this page.
- Because `1 + 1` is the last (and only) expression in the cell, Jupyter prints `2` below it — no
  `print` needed.

### Python you'll actually use
You don't need deep Python — just the everyday building blocks. The essentials:

This first cell introduces **variables** and Python's core **types**. A *variable* is a name you
attach to a value with the **`=`** sign so you can reuse it later. A *type* is the kind of value:
text, a whole number, a decimal, a true/false flag. You never declare types in Python — it figures
out the type from the value you assign.

```python
# Variables & types
name = "ShopFlow"        # str
orders = 40000           # int
aov = 84.50              # float
is_live = True           # bool
nothing = None           # null

# f-strings — the standard way to build text
print(f"{name} has {orders:,} orders, AOV ${aov:.2f}")

# Lists (ordered), dicts (key→value), sets (unique)
countries = ["US", "UK", "DE", "US"]
prices = {"keyboard": 89.0, "hub": 39.5}
unique_countries = set(countries)          # {'US','UK','DE'}

print(len(countries), prices["keyboard"], len(unique_countries))
```

**Read it step by step:**

- **`name = "ShopFlow"`** — assign the **string** (`str`) `"ShopFlow"` to the variable `name`.
  A string is text, always wrapped in quotes.
- **`orders = 40000`** — an **integer** (`int`): a whole number, no decimal point.
- **`aov = 84.50`** — a **float**: a number *with* a decimal point ("floating-point"). The `.50`
  is what makes it a float rather than an int.
- **`is_live = True`** — a **boolean** (`bool`): one of the two values `True` or `False`. Booleans
  are how Python answers yes/no questions and drive `if` statements.
- **`nothing = None`** — **`None`** is Python's "no value" / "nothing here" marker (SQL's `NULL`).
  You'll use it constantly for missing or not-yet-computed data.
- **`print(f"{name} has {orders:,} orders, AOV ${aov:.2f}")`** — **`print(...)`** displays text on
  screen. The **`f"..."`** is an **f-string** (formatted string): any `{...}` inside it is replaced
  by the value of the code inside the braces. So `{name}` becomes `ShopFlow`. The bits after the
  colon are **format specifiers**: `{orders:,}` adds thousands separators (`40,000`), and
  `{aov:.2f}` shows the float with exactly 2 decimal places (`84.50`).
- **`countries = ["US", "UK", "DE", "US"]`** — a **list**: an *ordered* collection written in
  square brackets `[...]`. Order is preserved and duplicates are allowed (note `"US"` appears
  twice).
- **`prices = {"keyboard": 89.0, "hub": 39.5}`** — a **dict** (dictionary): a set of **key → value**
  pairs written in curly braces `{...}`. Here the keys are product names and the values are prices.
  A dict is a lookup table: give it a key, get back its value.
- **`unique_countries = set(countries)`** — a **set**: an *unordered* collection of *unique* items.
  Passing the list to **`set(...)`** throws away the duplicate `"US"`, leaving `{'US','UK','DE'}`.
- **`print(len(countries), prices["keyboard"], len(unique_countries))`** — three things at once:
  **`len(...)`** returns how many items a collection holds (`4` countries); **`prices["keyboard"]`**
  looks a value up in the dict *by its key* (`89.0`); and `len(unique_countries)` is `3` because the
  duplicate was removed. `print` accepts several arguments separated by commas and prints them with
  spaces between.

!!! info "int vs float — and why it matters for data"
    Money and measurements are almost always **floats** (`84.50`), while counts and IDs are
    **ints** (`40000`, `order_id`). Mixing them is fine — Python auto-promotes `int` to `float` in
    arithmetic — but when you load data later, knowing a column's type tells you whether you can do
    math on it or must clean it first.

**Control flow & functions** — the logic you'll wrap around data:

A **function** is a named, reusable block of logic. You define one with **`def`**, give it
**arguments** (inputs), and it can hand back a result with **`return`**. This cell defines a
function that labels an amount, then a **`for` loop** that runs it over several values.

!!! note "Indentation *is* the syntax in Python"
    Python has no `{ }` or `end` to mark blocks — it uses **indentation** (the leading spaces). The
    lines indented under `def`, `if`, or `for` are the "body" of that block. Get the indentation
    wrong and the code won't run, so keep it consistent (4 spaces is the convention). Notice how the
    **colon `:`** at the end of a line always introduces an indented block below it.

```python
def revenue_band(amount):
    if amount >= 1000:
        return "large"
    elif amount >= 100:
        return "medium"
    return "small"

for amt in [50, 250, 5000]:
    print(amt, "→", revenue_band(amt))
```

**Read it step by step:**

- **`def revenue_band(amount):`** — define a function named `revenue_band` that takes one
  **argument** called `amount`. Inside the function, `amount` stands for whatever value the caller
  passes in.
- **`if amount >= 1000:`** — an **`if`** test. `>=` means "greater than or equal to"; it produces a
  boolean. If the test is `True`, Python runs the indented line below.
- **`return "large"`** — **`return`** immediately hands a value back to the caller and stops the
  function. So if `amount` is at least 1000, the function's answer is `"large"` and nothing else
  runs.
- **`elif amount >= 100:`** — **`elif`** ("else if") is checked *only when the previous `if` was
  `False`*. So this branch means "not ≥ 1000, but ≥ 100" → `"medium"`.
- **`return "small"`** — this line has no `if` in front of it, so it's the fall-through: if neither
  test matched, the amount is under 100, and the function returns `"small"`. (You could write this
  as `else:`; leaving it un-indented after the `return`s works the same way.)
- **`for amt in [50, 250, 5000]:`** — a **`for` loop** walks through a collection one item at a
  time. Each pass, the variable `amt` takes the next value from the list (`50`, then `250`, then
  `5000`), and the indented body runs once per value.
- **`print(amt, "→", revenue_band(amt))`** — for each amount, print the number, an arrow, and the
  result of *calling* `revenue_band(amt)`. Calling a function means writing its name followed by
  `(...)` with the input inside.

**Comprehensions** — the Pythonic one-liner for transforming a collection (you'll see this
everywhere):

A **list comprehension** builds a new list from an existing one in a single expression. Read it
as a compressed `for` loop: `[ <what to keep> for <item> in <collection> if <condition> ]`. The
`if` part is optional and *filters*; the front part *transforms* each item.

```python
amounts = [50, 250, 5000, 80]
big = [a for a in amounts if a >= 100]          # filter → [250, 5000]
bands = [revenue_band(a) for a in amounts]       # map    → ['small','medium','large','small']
print(big, bands)
```

**Read it step by step:**

- **`amounts = [50, 250, 5000, 80]`** — a plain list of numbers to work on.
- **`big = [a for a in amounts if a >= 100]`** — walk over every `a` in `amounts`, and keep only
  the ones where `a >= 100`. This is a **filter**: `50` and `80` drop out, leaving `[250, 5000]`.
- **`bands = [revenue_band(a) for a in amounts]`** — walk over every `a` and put *the result of*
  `revenue_band(a)` into the new list. There's no `if`, so nothing is filtered — every item is
  **transformed** ("mapped") through the function, giving one label per amount.
- **`print(big, bands)`** — print both lists.

!!! tip "Why comprehensions matter for data engineering"
    *Filter* and *map* are the two operations you do constantly to data — a comprehension is the
    quickest way to express them on small collections. The very same "filter, then transform"
    thinking is exactly what SQL's `WHERE` + `SELECT` (Unit {unit:unit2/intro.md}) and pandas/Spark do at scale next.

**Handling errors** — real pipelines hit bad data; catch it instead of crashing:

When Python hits something it can't do — like turning the word `"oops"` into a number — it raises
an **exception** and, by default, stops the whole program. A **`try` / `except`** block lets you
*attempt* risky code and *catch* the failure gracefully instead of crashing.

```python
def to_float(x):
    try:
        return float(x)
    except (ValueError, TypeError):
        return None          # a "TRY_CAST" in Python (like Unit {lesson:unit2/conditional.md})

print(to_float("12.5"), to_float("oops"))       # 12.5 None
```

**Read it step by step:**

- **`def to_float(x):`** — a function that tries to convert its input `x` into a float.
- **`try:`** — "attempt the indented code below; if it raises an error, don't crash — jump to the
  `except`."
- **`return float(x)`** — **`float(...)`** converts a value to a float. `float("12.5")` succeeds and
  returns `12.5`; `float("oops")` fails and raises an error.
- **`except (ValueError, TypeError):`** — catch these two specific error types. `float("oops")`
  raises a `ValueError`; passing something non-convertible (like `None`) raises a `TypeError`.
  Listing them in `( )` catches either one.
- **`return None`** — when conversion fails, hand back **`None`** instead of blowing up. This is the
  Python equivalent of SQL's `TRY_CAST` from Unit {lesson:unit2/conditional.md}: bad values become "nothing" rather than
  errors.
- **`print(to_float("12.5"), to_float("oops"))`** — the good input returns `12.5`; the bad input is
  caught and returns `None`.

!!! warning "Catch *specific* errors, not everything"
    It's tempting to write a bare `except:` that swallows *every* error — but that also hides bugs
    like typos and missing variables. Naming the exact exceptions you expect (`ValueError`,
    `TypeError`) means real, unexpected problems still surface loudly.

## Lab
In a Jupyter cell, build a tiny "orders" dataset as a list of dicts (the shape data often
arrives in from an API) and summarise it with plain Python:

A **list of dicts** is *the* everyday shape of data in Python: an outer list holds many records,
and each record is a dict of `field → value`. This is exactly what a web API hands back as JSON, so
learning to slice through it is a core DE skill. This one cell then computes two summaries — a total
and a per-country breakdown — using only what you've seen so far.

```python
orders = [
    {"order_id": 1, "country": "US", "amount": 120.0, "status": "delivered"},
    {"order_id": 2, "country": "UK", "amount": 40.0,  "status": "cancelled"},
    {"order_id": 3, "country": "US", "amount": 300.0, "status": "delivered"},
    {"order_id": 4, "country": "DE", "amount": 90.0,  "status": "delivered"},
]

# total delivered revenue
delivered = [o for o in orders if o["status"] == "delivered"]
total = sum(o["amount"] for o in delivered)
print(f"delivered revenue: ${total:,.2f}")

# revenue per country (a dict accumulator — the manual version of GROUP BY)
by_country = {}
for o in delivered:
    by_country[o["country"]] = by_country.get(o["country"], 0) + o["amount"]
print(by_country)
```

**Read it step by step:**

- **`orders = [ {...}, {...}, ... ]`** — a **list of dicts**. The list has 4 items; each item is one
  order, described by a dict with the keys `order_id`, `country`, `amount`, and `status`.
- **`delivered = [o for o in orders if o["status"] == "delivered"]`** — a **comprehension** that
  keeps only the orders whose status is `"delivered"`. Inside, `o` is one order dict, and
  **`o["status"]`** looks up that dict's status by key. **`==`** tests equality (note: two equals
  signs to *compare*, one to *assign*). Order 2 (cancelled) is dropped.
- **`total = sum(o["amount"] for o in delivered)`** — **`sum(...)`** adds up numbers. The bit inside
  is a **generator expression** (a comprehension without the `[ ]`) that pulls out `o["amount"]`
  from each delivered order, one at a time, and `sum` totals them: `120 + 300 + 90 = 510`.
- **`print(f"delivered revenue: ${total:,.2f}")`** — an f-string again; `{total:,.2f}` formats the
  number with thousands separators and 2 decimals → `$510.00`.
- **`by_country = {}`** — start an **empty dict**. We'll fill it up as an *accumulator*: keys will be
  countries, values will be running revenue totals.
- **`for o in delivered:`** — loop over each delivered order.
- **`by_country[o["country"]] = by_country.get(o["country"], 0) + o["amount"]`** — the heart of the
  group-by. **`by_country.get(o["country"], 0)`** reads the country's current total, returning `0`
  if we haven't seen that country yet (the second argument to **`.get`** is the default when the key
  is missing — this avoids a crash on the first sighting). We add this order's amount and store it
  back under the country key. Over the loop, each country's revenue accumulates.
- **`print(by_country)`** — show the finished map, e.g. `{'US': 420.0, 'DE': 90.0}`.

!!! note "Truthiness and `None` — Python's idea of yes/no"
    In an `if`, Python treats many values as "falsy": `None`, `0`, `""`, and an empty list/dict/set
    all count as `False`, while anything with content counts as `True`. That's why patterns like
    `if by_country:` (used in the Challenge) mean "if the dict has anything in it." It's a handy
    shorthand — but be careful, since a legitimate `0` also reads as falsy.

That `by_country` loop is a **GROUP BY** done by hand. It works — but it's verbose and slow on
real volumes. In [3.2](pandas.md) you'll do the same thing in one line with **pandas**, and in
Spark ([Unit 4](../unit4/fundamentals.md)) at any scale.

## Challenge
Write a function `summarise(orders)` that returns a dict with `count`, `delivered_revenue`, and
`top_country` (the country with the most delivered revenue). Test it on the sample above.

The goal here is to *package* the logic you just wrote into one reusable **function** that takes any
orders list and returns a tidy summary dict — the kind of building block a pipeline calls over and
over. Try it yourself first; the walkthrough below unpacks every line.

??? note "Solution"
    ```python
    def summarise(orders):
        delivered = [o for o in orders if o["status"] == "delivered"]
        by_country = {}
        for o in delivered:
            by_country[o["country"]] = by_country.get(o["country"], 0) + o["amount"]
        top = max(by_country, key=by_country.get) if by_country else None
        return {
            "count": len(orders),
            "delivered_revenue": sum(o["amount"] for o in delivered),
            "top_country": top,
        }

    print(summarise(orders))
    # {'count': 4, 'delivered_revenue': 510.0, 'top_country': 'US'}
    ```

    **Read it step by step:**

    - **`def summarise(orders):`** — a function taking the orders list as its argument.
    - **`delivered = [...]`** and the **`for` loop** — the same filter and dict-accumulator group-by
      you built in the Lab, now living inside the function.
    - **`top = max(by_country, key=by_country.get) if by_country else None`** — **`max(...)`** finds
      the largest item; `key=by_country.get` tells it to compare countries *by their revenue* rather
      than alphabetically, so it returns the country with the most revenue. The trailing
      **`if by_country else None`** is a *conditional expression* ("ternary"): use the `max` result
      **if** `by_country` has anything in it, **otherwise** fall back to `None` — because `max` of an
      empty dict would crash. This is the truthiness idea from the Lab in action.
    - **`return { "count": ..., "delivered_revenue": ..., "top_country": top }`** — build and return
      a **dict** as the result. `len(orders)` is the record count, `sum(...)` is the revenue, and
      `top` is the winning country. Returning a dict gives callers named fields instead of a
      hard-to-remember tuple.
    - **`print(summarise(orders))`** — call the function on the Lab's `orders` (still in memory,
      thanks to notebook state persistence!) and print the summary dict.

!!! tip "🎯 The same Python runs on Azure, Databricks, Snowflake & Fabric"
    Notebooks and Python are universal: **Databricks**, **Microsoft Fabric**, and **Azure
    Synapse** all give you Jupyter-style notebooks running the same Python; **Snowflake** runs
    Python in **Snowpark**/Snowsight notebooks. The language you learn here is 100% portable —
    only the data connectors differ.

## Key terms, at a glance
| Term | Plain meaning |
|---|---|
| **Notebook / cell** | Interactive doc; run code in chunks, see output inline |
| **State persists** | Variables live on across cells; run order = the order you ran them |
| **Variable (`=`)** | A name bound to a value; `=` assigns, `==` compares |
| **`int` / `float`** | Whole number / number with a decimal point |
| **`str`** | Text, in quotes |
| **`bool`** | `True` or `False` |
| **`None`** | Python's "no value" marker (SQL's `NULL`) |
| **Truthy / falsy** | `None`, `0`, `""`, empty collections read as `False` in an `if` |
| **list / dict / set** | Ordered items / key→value map / unique items |
| **Indexing (`d["key"]`)** | Look a value up by its key (dict) or position (list) |
| **`.get(key, default)`** | Dict lookup that returns a default instead of crashing |
| **f-string** | `f"{var}"` — the standard way to build text; `:,.2f` formats it |
| **`if` / `elif` / `else`** | Branch: run different code depending on a condition |
| **`for` loop** | Repeat the body once per item in a collection |
| **Function (`def`, `return`)** | Reusable named block; takes arguments, returns a result |
| **Comprehension** | `[f(x) for x in xs if cond]` — filter+map in one line |
| **`try/except`** | Catch errors instead of crashing (Python's TRY_CAST) |
| **`len` / `sum` / `max`** | Count items / add numbers / find the largest |
| **Indentation** | Leading spaces mark a block; the `:` opens it |

## You can now…
- Run code in Jupyter cells and read inline output
- Use Python's core types, f-strings, comprehensions, functions, and error handling
- Do a GROUP-BY-by-hand — and see why pandas/Spark will replace it next
