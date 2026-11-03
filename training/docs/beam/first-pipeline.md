# Your first pipeline: PCollections and transforms

## Concept
**Apache Beam** is a library for writing data pipelines, in Python (or Java, Go). You describe
**what** happens to the data, step by step. A **runner** decides **where** it runs. Google's
**Dataflow** is the best-known runner. It's where Beam came from, and Beam's words are Dataflow's
words. The same code runs on all runners:

| Runner | Runs your pipeline | In this lab |
|---|---|---|
| **DirectRunner / Prism** (the local runner) | inside your own machine, one process | `lab_beam("local")`, in your Jupyter |
| **DataflowRunner** | on Google Cloud's worker VMs, as a **Dataflow job** | the GCP unit ([Dataflow](../gcp/dataflow.md)) |
| **FlinkRunner** | on a Flink cluster's workers | `lab_beam("flink")`, the next lesson |

Three words carry all of Beam:

- **Pipeline**: the whole job, from reading to writing.
- **PCollection**: a dataset flowing between steps, a *parallel collection*. Its elements can be
  anything: strings, tuples, dicts. A PCollection is never changed; each step makes a new one.
- **PTransform** (transform): a step. It takes a PCollection and gives a new one: `Map`,
  `Filter`, `ReadFromText` …

Every pipeline has the same shape:

```python
with beam.Pipeline(options=…) as p:          # build the pipeline …
    (p
     | "Step name" >> beam.Create([...])      # … step by step: | applies a transform,
     | "Next step" >> beam.Map(some_function))#   "name" >> gives it a name (unique in the pipeline)
                                              # leaving the with block runs it, and waits
```

Nothing runs while you write the steps. Beam builds the **graph** of steps first, and the runner
runs it when the `with` block ends. That's why Dataflow can show you the job graph before any data
moves.

## Lab
Open a notebook. `beam` is Apache Beam; `lab_beam()` gives the pipeline its options, including
your own key for `s3://` paths in your bucket.

### 1 · Hello, Beam: `Create` and `Map`
```python
import apache_beam as beam

with beam.Pipeline(options=lab_beam("local")) as p:
    (p
     | "Some words" >> beam.Create(["lake", "beam", "dataflow"])
     | "Upper case" >> beam.Map(str.upper)
     | "Show" >> beam.Map(print))
```
```text
LAKE
BEAM
DATAFLOW
```

- **`beam.Create(list)`** turns a Python list into a PCollection: a source for trying things out.
- **`beam.Map(f)`** calls `f` on every element and keeps what it returns: one in, one out.
  `str.upper` is Python's own function; `print` returns nothing, it just shows each element.

### 2 · Keep some, split some: `Filter` and `FlatMap`
One PCollection can feed several steps. Here `products` feeds two branches:

```python
lines = ["P001,Wireless Mouse,Electronics,25.00", "P004,A5 Notebook,STATIONERY,4.50",
         "P007,Bluetooth Speaker,ELECTRONICS,45.00"]
with beam.Pipeline(options=lab_beam("local")) as p:
    products = p | "Products" >> beam.Create(lines)
    (products
     | "Fields" >> beam.Map(lambda line: line.split(","))
     | "Over 10" >> beam.Filter(lambda f: float(f[3]) > 10)
     | "Name and category" >> beam.Map(lambda f: (f[1], f[2].title()))
     | "Show" >> beam.Map(print))
    (products
     | "Words in names" >> beam.FlatMap(lambda line: line.split(",")[1].split())
     | "Show words" >> beam.Map(print))
```
```text
Wireless
Mouse
('Wireless Mouse', 'Electronics')
A5
Notebook
Bluetooth
Speaker
('Bluetooth Speaker', 'Electronics')
```

- **`beam.Filter(f)`** keeps the elements for which `f` is true (the notebook, at 4.50, is gone).
- **`beam.FlatMap(f)`**: `f` returns a list, and each item becomes its own element: one in, many out.
- **`.title()`** makes `ELECTRONICS` → `Electronics`, so the case doesn't matter any more.
- The two branches' lines are **mixed**. The runner runs independent steps as it likes, and a
  PCollection has no order. Never rely on the order of elements.

### 3 · A real file: quantity per day
ShopFlow's `orders.csv` is in your bucket (`files/source/shopflow/`): an `order_id`, the day,
customer, product and a `quantity`, which is sometimes empty.

```python
def parse(line):
    order_id, day, customer_id, product_id, qty = line.split(",")
    return {"order_id": order_id, "day": day, "product_id": product_id, "qty": qty}

with beam.Pipeline(options=lab_beam("local")) as p:
    orders = (p
              | "Read" >> beam.io.ReadFromText("s3://<you>-lake/files/source/shopflow/orders.csv", skip_header_lines=1)
              | "Parse" >> beam.Map(parse))
    (orders
     | "With a quantity" >> beam.Filter(lambda o: o["qty"] != "")
     | "Day, quantity" >> beam.Map(lambda o: (o["day"], int(o["qty"])))
     | "Sum per day" >> beam.CombinePerKey(sum)
     | "Show" >> beam.Map(print))
    (orders
     | "No quantity" >> beam.Filter(lambda o: o["qty"] == "")
     | "Count them" >> beam.combiners.Count.Globally()
     | "Show count" >> beam.Map(lambda n: print("orders without a quantity:", n)))
```
```text
('2026-09-01', 8)
('2026-09-02', 12)
('2026-09-03', 15)
orders without a quantity: 3
```

- **`beam.io.ReadFromText(path, skip_header_lines=1)`**: every line of the file is an element.
  `s3://` reaches your bucket; a folder pattern like `…/*.csv` reads many files.
- **Key/value pairs**: a tuple `(key, value)`. Beam's grouping steps work on them.
- **`beam.CombinePerKey(sum)`** groups by key and combines each key's values with `sum`: like
  `GROUP BY day` + `SUM(quantity)` in SQL.
- **`beam.combiners.Count.Globally()`** counts the whole PCollection: one number.

### 4 · Write the result
**`beam.io.WriteToText`** writes a PCollection as text files, one line per element:

```python
with beam.Pipeline(options=lab_beam("local")) as p:
    (p
     | "Read" >> beam.io.ReadFromText("s3://<you>-lake/files/source/shopflow/orders.csv", skip_header_lines=1)
     | "Parse" >> beam.Map(parse)
     | "With a quantity" >> beam.Filter(lambda o: o["qty"] != "")
     | "Day, quantity" >> beam.Map(lambda o: (o["day"], int(o["qty"])))
     | "Sum per day" >> beam.CombinePerKey(sum)
     | "As CSV" >> beam.MapTuple(lambda day, qty: f"{day},{qty}")
     | "Write" >> beam.io.WriteToText("s3://<you>-lake/files/beam/quantity_per_day", file_name_suffix=".csv",
                                      header="day,quantity", num_shards=1))
```

**📁 My files → `files/beam/`** now has `quantity_per_day-00000-of-00001.csv`:

```text
day,quantity
2026-09-01,8
2026-09-02,12
2026-09-03,15
```

- **`beam.MapTuple(f)`** is `Map` for tuples: `f` gets the key and the value as two arguments.
- The path is a **prefix**. Beam writes **shards**, one file per part of the data, named
  `-00000-of-0000N`. On many workers each one writes its own shard. `num_shards=1` asks for one
  file, which is fine for small results.

!!! info "Same code, other runners"
    Change `lab_beam("local")` to `lab_beam("flink", parallelism=4)` and this pipeline runs on the
    lab's Flink cluster ([next lesson](workers.md)). With Google's options instead, it runs as a
    Dataflow job ([Dataflow](../gcp/dataflow.md)). Nothing else changes.

## Check yourself
1. What's the difference between a PCollection and a transform?
2. `beam.Map(lambda line: line.split())` vs `beam.FlatMap(lambda line: line.split())` on
   `"a b c"`: what comes out of each?
3. Why did the two branches in step 2 print their lines mixed together?

??? success "Answers"
    1. A PCollection is data (it never changes). A transform is a step that takes one and gives a
       new one.
    2. `Map`: one element, the list `['a', 'b', 'c']`. `FlatMap`: three elements, `'a'`, `'b'`, `'c'`.
    3. The branches don't depend on each other, so the runner runs them side by side. A PCollection
       has no order.

## You can now…
- build and run a Beam pipeline on the local runner;
- use `Create`, `Map`, `Filter`, `FlatMap`, `MapTuple`, `CombinePerKey` and `Count`;
- read files from your bucket and write the result back as shards.
