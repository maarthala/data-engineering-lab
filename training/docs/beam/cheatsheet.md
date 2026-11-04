# Beam cheat sheet: the transforms you use most

## Concept
The building blocks you reach for in almost every Beam pipeline, each with one small example and
what it prints. Every example runs on the local runner in a notebook: copy the **setup** cell first,
then any example. The other Beam lessons show them in real pipelines; this page is to look things up.

The shape of every example: a `with` block builds the pipeline, and leaving it runs the pipeline
([Your first pipeline](first-pipeline.md)). Each step needs a **name that's unique** in its
pipeline (`"Name" >> transform`); the examples leave short ones out where a step is used once.

!!! tip "The order of printed lines"
    A PCollection has no order, so lines can print in another order than here, and branches mix.
    The examples sort where it helps.

## Setup
```python
import apache_beam as beam
from apache_beam import window

lines = ["P001,Wireless Mouse,Electronics,25.00", "P004,A5 Notebook,STATIONERY,4.50",
         "P007,Bluetooth Speaker,ELECTRONICS,45.00", "P002,USB-C Cable,Electronics,9.50"]
sales = [("web", 120.5), ("app", 45.0), ("web", 30.0), ("marketplace", 60.0), ("app", 15.0)]
```

## Pipeline: build and run
```python
with beam.Pipeline(options=lab_beam("local")) as p:     # leaving the block runs it, and waits
    p | beam.Create(["lake", "beam"]) | beam.Map(str.upper) | beam.Map(print)
```
```text
LAKE
BEAM
```

Without `with`, to build it over several cells, and run it yourself:

```python
p = beam.Pipeline(options=lab_beam("local"))
words = p | "Words" >> beam.Create(["lake", "beam"])
words | "Show" >> beam.Map(print)
p.run().wait_until_finish()                              # nothing runs before this line
```
```text
lake
beam
```

- **`lab_beam("local")`** runs in your notebook; `lab_beam("flink", parallelism=4)` on the Flink unit
  ([Run it on workers](workers.md)).
- A step added after the pipeline ran never runs; Jupyter then shows `<PCollection …>`.

## In and out
| Transform | What it does |
|---|---|
| `beam.Create(list)` | a PCollection from a Python list: for trying things out |
| `beam.io.ReadFromText(path, skip_header_lines=1)` | one element per line; `path` may be a pattern (`…/*.csv`); `s3://<you>-lake/…` is your bucket |
| `beam.io.WriteToText(prefix, file_name_suffix=".csv", num_shards=1, header=…)` | one line per element, into shard files `prefix-00000-of-00001.csv` |
| `beam.io.WriteToParquet(prefix, schema, file_name_suffix=".parquet")` | dicts as Parquet, typed by a PyArrow schema ([A real pipeline](silver.md)) |
| `beam.io.ReadFromParquet(path)` | Parquet rows as dicts |
| `fileio.WriteToFiles(path, …)` | files from a **stream** ([A streaming pipeline](kafka.md)) |

## Element by element
### `Map`: one in, one out
```python
with beam.Pipeline(options=lab_beam("local")) as p:
    (p | beam.Create(lines)
       | beam.Map(lambda line: line.split(",")[1])
       | beam.Map(print))
```
```text
Wireless Mouse
A5 Notebook
Bluetooth Speaker
USB-C Cable
```

`Map` passes on what the function **returns**: `beam.Map(print)` passes on `None`, so put it last
(or use `peek`, below).

### `MapTuple`: `Map` for tuples
```python
with beam.Pipeline(options=lab_beam("local")) as p:
    (p | beam.Create(sales)
       | beam.MapTuple(lambda channel, amount: f"{channel}: {amount:.2f}")
       | beam.Map(print))
```
```text
web: 120.50
app: 45.00
web: 30.00
marketplace: 60.00
app: 15.00
```

The tuple's parts arrive as separate arguments: no `x[0]`, `x[1]`.

### `FlatMap`: one in, many out
```python
with beam.Pipeline(options=lab_beam("local")) as p:
    (p | beam.Create(["lake beam", "dataflow"])
       | beam.FlatMap(lambda text: text.split())
       | beam.Map(print))
```
```text
lake
beam
dataflow
```

The function returns a list (or yields); each item becomes its own element. `FlatMapTuple` is the
same for tuples.

### `Filter`: keep some
```python
with beam.Pipeline(options=lab_beam("local")) as p:
    (p | beam.Create(lines)
       | beam.Map(lambda line: line.split(","))
       | beam.Filter(lambda f: float(f[3]) > 10)
       | beam.Map(lambda f: f[1])
       | beam.Map(print))
```
```text
Wireless Mouse
Bluetooth Speaker
```

### `ParDo` and `DoFn`: your own step, with several outputs
```python
class CheckPrice(beam.DoFn):
    def process(self, line):
        f = line.split(",")
        if float(f[3]) >= 10:
            yield f[1]                                            # the main output
        else:
            yield beam.pvalue.TaggedOutput("cheap", f[1])         # a second output

with beam.Pipeline(options=lab_beam("local")) as p:
    out = p | beam.Create(lines) | beam.ParDo(CheckPrice()).with_outputs("cheap", main="ok")
    out.ok    | "Show ok"    >> beam.Map(lambda x: print("ok:", x))
    out.cheap | "Show cheap" >> beam.Map(lambda x: print("cheap:", x))
```
```text
ok: Wireless Mouse
cheap: A5 Notebook
ok: Bluetooth Speaker
cheap: USB-C Cable
```

- Beam calls **`process`** for every element; `yield` sends one on, none drops it, several send
  several.
- **`with_outputs("cheap", main="ok")`** gives `out.ok` and `out.cheap`; use them, not `out`
  itself. A dead-letter output for bad rows works the same way ([A real pipeline](silver.md)).
- `setup(self)` runs once per worker: open a connection there, not in `process`.

### `Partition`: split into a fixed number of PCollections
```python
with beam.Pipeline(options=lab_beam("local")) as p:
    small, big = (p | beam.Create(sales)
                    | beam.Partition(lambda sale, n: 0 if sale[1] < 50 else 1, 2))
    small | "Show small" >> beam.Map(lambda s: print("small:", s))
    big   | "Show big"   >> beam.Map(lambda s: print("big:", s))
```
```text
big: ('web', 120.5)
small: ('app', 45.0)
small: ('web', 30.0)
big: ('marketplace', 60.0)
small: ('app', 15.0)
```

## Keys and values
A **key/value pair** is a tuple `(key, value)`. Grouping steps work on them.

| Transform | From → to |
|---|---|
| `beam.Map(lambda x: (key_of(x), x))` | make pairs (also `beam.WithKeys(key_of)`) |
| `beam.Keys()` / `beam.Values()` | `(k, v)` → `k` / `v` |
| `beam.KvSwap()` | `(k, v)` → `(v, k)` |

## Grouping and combining
### `GroupByKey`: all values of a key together
```python
with beam.Pipeline(options=lab_beam("local")) as p:
    (p | beam.Create(sales)
       | beam.GroupByKey()
       | beam.MapTuple(lambda channel, amounts: (channel, sorted(amounts)))
       | beam.Map(print))
```
```text
('web', [30.0, 120.5])
('app', [15.0, 45.0])
('marketplace', [60.0])
```

The values come as an iterable (`list(…)`, `sorted(…)`, `max(…)` work on it). On workers this is a
**shuffle**: every pair travels to the worker that owns its key.

### `CombinePerKey`: group and reduce in one
```python
with beam.Pipeline(options=lab_beam("local")) as p:
    (p | beam.Create(sales)
       | beam.CombinePerKey(sum)
       | beam.Map(print))
```
```text
('web', 150.5)
('app', 60.0)
('marketplace', 60.0)
```

Cheaper than `GroupByKey` + summing yourself: each worker combines its own part first, so little
travels ([Run it on workers](workers.md)). Any function of an iterable works: `sum`, `max`, `min`.

### Ready-made combiners
```python
from apache_beam.transforms import combiners

with beam.Pipeline(options=lab_beam("local")) as p:
    s = p | beam.Create(sales)
    s | "Count per key" >> combiners.Count.PerKey() | "Show count" >> beam.Map(lambda x: print("count:", x))
    s | "Mean per key"  >> combiners.Mean.PerKey()  | "Show mean"  >> beam.Map(lambda x: print("mean:", x))
    (s | "Amounts" >> beam.Values()
       | "Top 2" >> combiners.Top.Of(2) | "Show top" >> beam.Map(lambda x: print("top 2:", x)))
    (s | "All" >> combiners.Count.Globally() | "Show all" >> beam.Map(lambda x: print("elements:", x)))
```
```text
mean: ('marketplace', 60.0)
mean: ('web', 75.25)
mean: ('app', 30.0)
count: ('marketplace', 1)
count: ('web', 2)
count: ('app', 2)
top 2: [120.5, 60.0]
elements: 5
```

| Combiner | Gives |
|---|---|
| `Count.Globally()` · `Count.PerKey()` · `Count.PerElement()` | how many: in all · per key · per distinct element |
| `Mean.Globally()` · `Mean.PerKey()` | the average |
| `Top.Of(n)` · `Top.Largest(n)` · `Top.Smallest(n)` · `Top.PerKey(n)` | the n largest (or smallest), as a list |
| `beam.CombineGlobally(sum)` | one value from the whole PCollection |
| `combiners.ToList()` | the whole PCollection as one list (only in the global window: [Event time and windows](windows.md)) |
| `combiners.Sample.FixedSizeGlobally(n)` | n random elements, as a list: a quick look at big data |

### `Distinct`: drop duplicates
```python
with beam.Pipeline(options=lab_beam("local")) as p:
    (p | beam.Create(["web", "app", "web", "web"])
       | beam.Distinct()
       | beam.Map(print))
```
```text
web
app
```

It compares **whole elements**: use it on lines (strings) or tuples. Two rows that differ in one
field both stay; for "one row per key", group by the key and keep one (`GroupByKey` +
`MapTuple` with `max(rows, key=…)`).

## Several PCollections
### `Flatten`: one PCollection from several of the same kind
```python
with beam.Pipeline(options=lab_beam("local")) as p:
    eu = p | "EU" >> beam.Create(["NL", "DE"])
    us = p | "US" >> beam.Create(["US"])
    (eu, us) | beam.Flatten() | beam.Map(print)
```
```text
US
NL
DE
```

### `CoGroupByKey`: a join by key
```python
with beam.Pipeline(options=lab_beam("local")) as p:
    names  = p | "Customers" >> beam.Create([(1, "Ravi"), (2, "Anna")])
    orders = p | "Orders"    >> beam.Create([(1, 120.5), (1, 10.0), (3, 60.0)])
    ({"name": names, "orders": orders}
     | beam.CoGroupByKey()
     | beam.MapTuple(lambda cid, g: (cid, list(g["name"]), sorted(g["orders"])))
     | beam.Map(print))
```
```text
(1, ['Ravi'], [10.0, 120.5])
(3, [], [60.0])
(2, ['Anna'], [])
```

Every key from either side comes out, with what each side has for it: customer `2` has no orders,
order `3` no customer. Keep both lists, or only keys found on both sides (an inner join), as you need.

### Side inputs: a small dataset handed whole to a step
```python
with beam.Pipeline(options=lab_beam("local")) as p:
    rates = p | "Rates" >> beam.Create([("USD", 1.0), ("EUR", 1.08)])
    (p | "Prices" >> beam.Create([("EUR", 10.0), ("USD", 5.0)])
       | beam.Map(lambda price, fx: (price[0], price[1], round(price[1] * fx[price[0]], 2)),
                  fx=beam.pvalue.AsDict(rates))
       | beam.Map(print))
```
```text
('EUR', 10.0, 10.8)
('USD', 5.0, 5.0)
```

`AsDict` (pairs → a dict), `AsList` (→ a list), `AsSingleton` (one value, e.g. a total). For small
data only: every worker gets the whole of it ([A real pipeline](silver.md)). Use `Map` (or `ParDo`)
with a side input: `MapTuple` refuses one unless the argument has a default (*Side inputs must have
defaults for MapTuple*).

## Time and windows
```python
with beam.Pipeline(options=lab_beam("local")) as p:
    (p | beam.Create([("click", 5), ("click", 30), ("click", 70)])            # (what, seconds)
       | beam.Map(lambda e: window.TimestampedValue((e[0], 1), e[1]))       # its event time
       | beam.WindowInto(window.FixedWindows(60))                           # per minute
       | beam.CombinePerKey(sum)
       | beam.Map(print))
```
```text
('click', 2)
('click', 1)
```

The same key, counted per window: 2 in the first minute (seconds 5 and 30), 1 in the second (70).

| | Groups elements into |
|---|---|
| `window.TimestampedValue(x, seconds)` | gives an element its event time |
| `window.FixedWindows(size)` | back-to-back intervals |
| `window.SlidingWindows(size, period)` | overlapping intervals |
| `window.Sessions(gap)` | runs of activity per key, ended by a gap |
| `window.GlobalWindows()` | one window for everything (the default) |

Triggers and late data: [Event time and windows in Beam](windows.md).

## Spread the work: `Reshuffle`
`| beam.Reshuffle()` redistributes elements evenly over the workers, breaking a fused step that
one worker would do alone (one input file, for example): [Run it on workers](workers.md).

## Debug: look at elements in the middle
```python
def peek(x):
    print("→", x)
    return x                         # hand the same element on

with beam.Pipeline(options=lab_beam("local")) as p:
    (p | beam.Create(lines[:2])
       | "Peek" >> beam.Map(peek)
       | beam.Map(lambda line: line.split(",")[0])
       | beam.Map(print))
```
```text
→ P001,Wireless Mouse,Electronics,25.00
P001
→ P004,A5 Notebook,STATIONERY,4.50
P004
```

On the Flink unit, `print` goes to the workers' logs, not your notebook: write results to files there.

## Test: check a transform's output
```python
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

with TestPipeline() as p:
    out = p | beam.Create(lines) | beam.ParDo(CheckPrice()).with_outputs("cheap", main="ok")
    assert_that(out.cheap, equal_to(["A5 Notebook", "USB-C Cable"]), label="cheap ones")
print("test passed")
```
```text
test passed
```

`assert_that` fails the pipeline when the output differs (in any order). Run tests like this with
`pytest`, no cluster needed.

## Common errors
| Error | Why | Fix |
|---|---|---|
| `A transform with label "x" already exists` | two steps share a name | give each step its own name |
| `'NoneType' object has no attribute …` after `Map(print)` | `print` returns `None` | print last, or use `peek` |
| `<PCollection[…] …>` shown, nothing printed | the steps were built, the pipeline didn't run | build inside the `with` block, or `p.run()` |
| `'PCollection' object has no attribute 'ok'` | `.ok` taken from the wrong variable | use the result of `ParDo(…).with_outputs(…)` |
| `'_InvalidUnpickledPCollection' …` | a step chained right after `with_outputs(…)` | apply it to `out.ok`, not to `out` |
| `Unable to get filesystem from specified path … s3a://` | `s3a://` is Spark's scheme | `s3://` in Beam |
| `too many values to unpack` on CSV | a quoted comma, e.g. `"1,250.00"` | `next(csv.reader([line]))` instead of `split(",")` |
| `Unparseable argument … ipykernel_launcher.py` (a warning) | Beam reads the notebook kernel's arguments | harmless |

## Check yourself
1. `Map` or `FlatMap` to turn `"a b c"` into three elements?
2. Why is `CombinePerKey(sum)` cheaper than `GroupByKey` and summing the list yourself?
3. `Distinct` kept two rows for customer 2. Why, and what removes the second?

??? success "Answers"
    1. `FlatMap`: each item of the returned list becomes an element. `Map` would give one element,
       the list.
    2. Each worker sums its own part before the shuffle, so only partial sums travel, not every
       value.
    3. The two rows differ in some field, and `Distinct` compares whole elements. Group by
       `customer_id` and keep one row per key.

## You can now…
- pick the transform for a job: element by element, grouping, combining, joining, windows;
- split a step's output, hand a small dataset to a step, and merge PCollections;
- look at elements in the middle, and test a transform's output.
