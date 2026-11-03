# Run it on workers: the job, its graph and parallelism

## Concept
On the local runner a pipeline runs in one process. On **Dataflow** the same pipeline becomes a
**job**. Google starts **worker** VMs, the job's steps run as many parallel copies across them, and
the console shows the **job graph**. The lab has a small cluster that works the same way: the
**Flink unit**, with Beam's **FlinkRunner**. Dataflow's words, and what they are here:

| Dataflow | Here (Flink) | What it is |
|---|---|---|
| job | a Flink job, `<you>-<name>` | one run of a pipeline on the cluster |
| worker (VM) | a **TaskManager** | a worker process; it runs the steps' copies |
| worker threads | its **slots** (2 per TaskManager) | how many copies one worker runs at once |
| number of workers × threads | **parallelism** | how many copies of each step: `lab_beam("flink", parallelism=4)` |
| job graph | the Flink UI's job graph (**Tools → Flink UI**) | the steps as boxes, with the data flowing between them |
| **fusion** | a **chain** (`CHAIN …` in a box's name) | steps joined into one, so data passes between them in memory |
| **shuffle** | a **partition / group** edge | data re-sent between workers, by key |

**Fusion** is the runner's main trick. Consecutive steps that don't need to regroup the data
(Read → Parse → Map) run as one fused step on the same worker. Only a step that needs all values of
a key together (`GroupByKey`, `CombinePerKey`) forces a **shuffle**: every element travels to the
worker that owns its key.

That has a catch. A fused step's parallelism is limited by its **input**. One small file is one
piece of work, so one worker reads it and runs the whole fused chain alone while the others wait.
**`beam.Reshuffle()`** breaks the fusion on purpose: it redistributes the elements evenly across
all workers.

!!! info "Needs the Flink unit"
    Flink is a unit, off until a lab manager switches it on: on your laptop `make status` →
    **Flink** → **s**, on a team stack **Manage → ⚙️ Settings → Services → Flink**. The manager also
    sets its size there: TaskManagers × slots each (2 × 2 = 4 slots here). While it's off,
    `lab_beam("flink")` says so; `lab_beam("local")` always works.

## Lab
The data: `sample_orders.csv` in your bucket, 1,000 ShopFlow orders. Each line has an id, a
customer, a **channel** (web, app, marketplace, wholesale), a time, a status, a currency.

### 1 · The same pipeline, two runners
Orders per channel, as a function, so you can run it with either runner:

```python
import apache_beam as beam, time
SRC = "s3://<you>-lake/files/source/shopflow/sample_orders.csv"

def channel_counts(options, spread=False):
    with beam.Pipeline(options=options) as p:
        orders = (p | "Read" >> beam.io.ReadFromText(SRC, skip_header_lines=1)
                    | "Parse" >> beam.Map(lambda line: line.split(",")))
        if spread:
            orders = orders | "Spread" >> beam.Reshuffle()
        (orders
         | "Channel" >> beam.Map(lambda f: (f[2], 1))
         | "Count per channel" >> beam.CombinePerKey(sum)
         | "As text" >> beam.MapTuple(lambda ch, n: f"{ch},{n}")
         | "Write" >> beam.io.WriteToText(f"s3://<you>-lake/files/beam/channels_{'spread' if spread else 'plain'}",
                                          file_name_suffix=".csv"))

for runner, kw in (("local", {}), ("flink", {"parallelism": 4})):
    t0 = time.time()
    channel_counts(lab_beam(runner, name="channels", **kw))
    print(runner, "took", round(time.time() - t0), "s")
```
```text
local took 5 s
flink took 44 s
```

The cluster is slower here: starting a job (planning it, starting Beam's Python workers in every
slot) takes far longer than 1,000 rows of work. Dataflow is the same: a job takes minutes to start.
A cluster pays off when the work is bigger than the start-up.

**Tools → Flink UI** lists the job as **`<you>-channels`**, *FINISHED*. Click it for its graph.

### 2 · Read the job graph
The UI shows each box's records per subtask (slot). The same numbers come from Flink's REST API,
which the UI itself uses, so you can print them in the notebook:

```python
import requests
F = "http://flink-jobmanager:8081"

def job_steps(name):
    job = max((j for j in requests.get(f"{F}/jobs/overview").json()["jobs"] if j["name"] == name),
              key=lambda j: j["start-time"])          # the latest run of that name
    for v in requests.get(f"{F}/jobs/{job['jid']}").json()["vertices"]:
        subs = requests.get(f"{F}/jobs/{job['jid']}/vertices/{v['id']}").json()["subtasks"]
        print([s["metrics"]["read-records"] for s in subs], "→",
              [s["metrics"]["write-records"] for s in subs], v["name"][:70])

job_steps("<you>-channels")
```
```text
[0, 0, 0, 0] → [1, 0, 0, 0] DataSource (Impulse)
[0, 0, 0, 0] → [1, 0, 0, 0] DataSource (Impulse)
[1, 0, 0, 0] → [1, 0, 0, 0] CHAIN MapPartition (MapPartition at [3]Read/Read/{EmitSource, SDFBound
[1, 0, 0, 0] → [8, 0, 0, 0] CHAIN MapPartition (MapPartition at [4]{Read, Parse, Channel, Count pe
[1, 0, 1, 2] → [1, 0, 1, 2] GroupReduce (GroupReduce at Count per channel/Group)
[1, 0, 1, 2] → [1, 0, 1, 2] CHAIN MapPartition (MapPartition at [4]{Count per channel, As text, Wr
…
```
(The last lines, the `Write` steps, are left out.) Each list has one number per worker slot,
records in → records out. `Impulse` is how Beam starts a pipeline: one empty element.

- **`{Read, Parse, Channel, Count per channel}`** is one fused step. Its input was one
  file, so **slot 1 did all of it**, and the other three got nothing.
- It sent on **8** records, not 1,000. `CombinePerKey(sum)` sums **before** the shuffle, a partial
  count per channel, so little travels. Dataflow calls this **combiner lifting**.
- **`GroupReduce`** is the shuffle: the partial counts of each channel meet in one slot, which
  adds them up. Four channels, spread over slots 1, 3 and 4.

### 3 · Spread the work: `Reshuffle`
```python
channel_counts(lab_beam("flink", name="channels-spread", parallelism=4), spread=True)
job_steps("<you>-channels-spread")
```
```text
…
[0, 0, 0, 1] → [0, 0, 0, 1000] CHAIN MapPartition (MapPartition at [2]{Read, Parse}) -> FlatMap (Flat
[250, 250, 250, 250] → [250, 250, 250, 250] Partition
[250, 250, 250, 250] → [8, 8, 8, 8] CHAIN MapPartition (MapPartition at [2]{Channel, Count per channel}) -
[4, 0, 4, 8] → [1, 0, 1, 2] GroupReduce (GroupReduce at Count per channel/Group)
…
```

- Reading is still one slot (one file), but now it only reads and parses: 1,000 rows out.
- **`Partition`** (the Reshuffle) sends **250 to each slot**.
- **Each slot counts its own 250** in parallel, and sends 8 partial counts on.
- The shuffle (`GroupReduce`) merges 4 + 4 + 8 partials into the 4 totals.

The result, in three shards (`files/beam/channels_spread-0000N-of-00003.csv`):

```text
marketplace,160
app,278
web,424
wholesale,138
```

160 + 278 + 424 + 138 = 1,000. Same answer as the local runner, computed by four workers.

!!! tip "Where print goes"
    `beam.Map(print)` on the cluster prints in the workers, into their logs, not into your notebook.
    On a cluster, write results to files (or a table) and read those.

## Check yourself
1. Why did one slot do all the work in step 2, even with parallelism 4?
2. `CombinePerKey(sum)` sent 8 records into the shuffle instead of 1,000. How?
3. When is the Flink (or Dataflow) runner worth it, and when is the local runner better?

??? success "Answers"
    1. The steps were fused into one, and its input was a single file, one piece of work. Only one
       slot got it. `Reshuffle` (or many input files) spreads the work.
    2. Combiner lifting: each worker sums its own elements per key first, and only those partial sums
       are shuffled.
    3. The cluster when the data is big enough that dividing it over workers beats the start-up time
       (seconds to minutes). The local runner for small data, tests and developing the pipeline.

## You can now…
- run the same pipeline locally and on a cluster of workers, with a chosen parallelism;
- read a job graph: fused steps, shuffles, records per worker slot;
- explain fusion, shuffles and combiner lifting, and spread work with `Reshuffle`.
