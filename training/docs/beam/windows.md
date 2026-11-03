# Event time and windows in Beam

## Concept
[Stream processing in a notebook](../pubsub/stream-processing.md) brought in **event time** (when
something happened), **windows** and the **watermark** (how far event time has got, so a window can
close). Beam, and Dataflow with it, is built around them, with the same three kinds of window:

| Beam | Groups elements into | Example |
|---|---|---|
| **`FixedWindows(size)`** | back-to-back intervals | orders per 30 days |
| **`SlidingWindows(size, period)`** | overlapping intervals | orders in the last 90 days, every 30 days |
| **`Sessions(gap)`** | runs of activity per key, ended by a gap | a customer's buying spells |

Beam adds one more idea: **triggers**. A window doesn't have to give **one** result when it closes.
It can give an **on-time** result when the watermark passes its end, then a **late** result for
every late element that still comes, up to an **allowed lateness**. Late data updates the answer
instead of being lost.

In Beam, windows work the same in **batch**: give each element its event time, and a bounded file
is windowed just like a stream.

## Lab
The 1,000 timestamped `sample_orders` (each has an `order_ts`). The local runner is enough.

### 1 · Give each element its event time
```python
import apache_beam as beam
from apache_beam import window
from datetime import datetime, timezone

def ts(s): return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp()
def day(t): return datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%d")

class Show(beam.DoFn):                       # a result with its window
    def process(self, kv, w=beam.DoFn.WindowParam):
        yield f"{day(w.start.micros / 1e6)} – {day(w.end.micros / 1e6)}  {kv[0]}: {kv[1]}"

SRC = "s3://<you>-lake/files/source/shopflow/sample_orders.csv"
def orders(p):
    return (p | "Read" >> beam.io.ReadFromText(SRC, skip_header_lines=1)
              | "Fields" >> beam.Map(lambda l: l.split(","))
              | "Event time" >> beam.Map(lambda f: window.TimestampedValue(f, ts(f[3]))))
```

- **`window.TimestampedValue(element, seconds)`** gives an element its event time, here the order's
  `order_ts` as seconds since 1970. Read from a file, elements have no time of their own.
- **`w=beam.DoFn.WindowParam`** in `process` asks Beam for the element's window: its `start` and
  `end` (Beam counts time in microseconds, hence `.micros / 1e6`).

### 2 · Fixed windows: orders per 30 days
**`beam.WindowInto(window.FixedWindows(30 * 86400))`** puts every element in its 30-day window
(sizes are in seconds: 86,400 a day). After that, `CombinePerKey` counts **per window**:

```python
with beam.Pipeline(options=lab_beam("local")) as p:
    (orders(p)
     | "Fixed" >> beam.WindowInto(window.FixedWindows(30 * 86400))
     | "All" >> beam.Map(lambda f: ("orders", 1))
     | "Count" >> beam.CombinePerKey(sum)
     | "Show" >> beam.ParDo(Show())
     | "One window" >> beam.WindowInto(window.GlobalWindows())
     | "Collect" >> beam.combiners.ToList()
     | "Print" >> beam.Map(lambda xs: print("\n".join(sorted(xs)[:4]), f"\n… {len(xs)} windows")))
```
```text
2023-05-23 – 2023-06-22  orders: 26
2023-06-22 – 2023-07-22  orders: 45
2023-07-22 – 2023-08-21  orders: 40
2023-08-21 – 2023-09-20  orders: 34
… 25 windows
```

- The windows start on 23 May, not 1 June: fixed windows are counted from 1970, so every 30 days
  from then. They're not calendar months.
- **`beam.WindowInto(window.GlobalWindows())`** puts everything back into the one window that holds
  all the data. `ToList` collects across the whole PCollection, which only works there.
  `ToList` on windowed data fails with *Default values are not yet supported … not windowed by
  GlobalWindows*.

### 3 · Sliding windows: the last 90 days, every 30
**`window.SlidingWindows(90 * 86400, 30 * 86400)`**: 90-day windows, a new one every 30 days. So
each order falls in **three** of them:

```python
with beam.Pipeline(options=lab_beam("local")) as p:
    (orders(p)
     | "Sliding" >> beam.WindowInto(window.SlidingWindows(90 * 86400, 30 * 86400))
     | "All" >> beam.Map(lambda f: ("orders", 1))
     | "Count" >> beam.CombinePerKey(sum)
     | "Show" >> beam.ParDo(Show())
     | "One window" >> beam.WindowInto(window.GlobalWindows())
     | "Collect" >> beam.combiners.ToList()
     | "Print" >> beam.Map(lambda xs: print("\n".join(sorted(xs)[3:6]),
                                            f"\n… {len(xs)} windows, {sum(int(x.split(': ')[1]) for x in xs)} counted")))
```
```text
2023-06-22 – 2023-09-20  orders: 119
2023-07-22 – 2023-10-20  orders: 114
2023-08-21 – 2023-11-19  orders: 113
… 27 windows, 3000 counted
```

1,000 orders, counted 3,000 times: once in each of the three windows that cover them. A moving
total like this is what a "last 90 days" dashboard tile shows.

### 4 · Sessions: a customer's buying spells
**`window.Sessions(60 * 86400)`** is per **key**. It groups a customer's orders into one session
until 60 days pass without an order:

```python
with beam.Pipeline(options=lab_beam("local")) as p:
    (orders(p)
     | "Sessions" >> beam.WindowInto(window.Sessions(60 * 86400))
     | "Customer" >> beam.Map(lambda f: (f[1], 1))
     | "Orders per session" >> beam.CombinePerKey(sum)
     | "Show" >> beam.ParDo(Show())
     | "One window" >> beam.WindowInto(window.GlobalWindows())
     | "Collect" >> beam.combiners.ToList()
     | "Print" >> beam.Map(lambda xs: print(f"{len(xs)} sessions; the biggest:\n" +
                                            "\n".join(sorted(xs, key=lambda x: -int(x.split(': ')[1]))[:3]))))
```
```text
923 sessions; the biggest:
2023-09-06 – 2024-01-16  63: 3
2024-04-15 – 2024-07-26  75: 3
2025-01-21 – 2025-05-23  46: 3
```

Customer 63 bought three times between September and January, each order within 60 days of the
one before. A session ends 60 days after its last order (`end`). Most of the 661 customers bought
once per spell: 923 sessions for 1,000 orders.

### 5 · Triggers: on time, then late
A **`TestStream`** feeds a stream with a controlled watermark, to see a trigger work. Three orders
arrive in the 10:00 minute, the watermark moves past 10:01 (the minute is complete), and then one
more order for 10:00:30 arrives, **late**:

```python
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.trigger import AfterWatermark, AfterCount, AccumulationMode

T0 = ts("2026-11-03 10:00:00")
events = (TestStream()
          .advance_watermark_to(T0)
          .add_elements([window.TimestampedValue(("orders", 1), T0 + s) for s in (5, 20, 40)])
          .advance_watermark_to(T0 + 70)                                     # past 10:01
          .add_elements([window.TimestampedValue(("orders", 1), T0 + 30)])   # happened 10:00:30
          .advance_watermark_to(T0 + 200)
          .advance_watermark_to_infinity())

class Pane(beam.DoFn):
    def process(self, kv, w=beam.DoFn.WindowParam, pane=beam.DoFn.PaneInfoParam):
        start = datetime.fromtimestamp(w.start.micros / 1e6, timezone.utc).strftime("%H:%M")
        timing = {0: "EARLY", 1: "ON_TIME", 2: "LATE"}.get(pane.timing, "UNKNOWN")
        print(f"window {start}: {kv[1]} orders · {timing}")

with beam.Pipeline(options=lab_beam("local", streaming=True)) as p:
    (p | events
       | beam.WindowInto(window.FixedWindows(60),
                         trigger=AfterWatermark(late=AfterCount(1)),
                         allowed_lateness=300,
                         accumulation_mode=AccumulationMode.ACCUMULATING)
       | beam.CombinePerKey(sum)
       | beam.ParDo(Pane()))
```
```text
window 10:00: 3 orders · ON_TIME
window 10:00: 4 orders · LATE
```

- **`trigger=AfterWatermark(late=AfterCount(1))`**: give the window's result when the watermark
  passes its end (on time), then again for every late element.
- **`allowed_lateness=300`**: late elements up to 5 minutes after the window's end still count.
  Later ones are dropped, like Spark's watermark.
- **`AccumulationMode.ACCUMULATING`**: each new result includes the earlier elements, so you get 3,
  then 4. **`DISCARDING`** would give 3, then just the 1 new one.
- **`pane=beam.DoFn.PaneInfoParam`**: which firing a result came from. Each firing is a **pane**.
- **`lab_beam("local", streaming=True)`**: run it as a stream (`TestStream` needs that).

Spark's watermark gives **one** result per window and drops what comes after it. Beam can **correct
a result** when late data comes. That's what Dataflow streaming jobs use for numbers that have to
be right, not only fast.

## Check yourself
1. With `SlidingWindows(90 days, 30 days)`, why were 1,000 orders counted 3,000 times?
2. Why are sessions defined per key, and what ends one?
3. A window fired *ON_TIME* with 3, then *LATE* with 4. What would `DISCARDING` give, and what does
   `allowed_lateness` decide?

??? success "Answers"
    1. Each 90-day window overlaps the next by 60 days, so every order lies in three windows and is
       counted in each.
    2. A session is one key's run of activity (a customer's orders). It ends when no element for that
       key comes within the gap.
    3. 3, then 1: only the new element in the late pane. `allowed_lateness` decides how long after the
       window's end late elements still change it; later ones are dropped.

## You can now…
- give elements their event time, and window them: fixed, sliding, session;
- go back to the global window to combine across all windows;
- use a trigger with allowed lateness, to get on-time and late results for the same window.
