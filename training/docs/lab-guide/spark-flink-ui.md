# ⚡ Spark UI and Flink UI

The lab has two compute clusters, each with a web UI that shows what runs on it. **Tools → Spark UI**
shows the Spark cluster: your notebooks' and DAGs' Spark work. **Tools → Flink UI** shows the Flink
cluster: Apache Beam pipelines run with `lab_beam("flink")`. Both clusters are shared by the whole lab.

## The two units
| | Spark unit | Flink unit |
|---|---|---|
| Runs | Spark: the `spark` session in notebooks, `lab_spark()` in DAG tasks | Apache Beam pipelines on workers with slots |
| Parts | a Spark master, workers and one Spark Connect server | a JobManager, TaskManagers, Beam's job server and expansion service |
| Without it | notebooks say **⚠ `spark` not created** | `lab_beam("local")` still runs Beam inside your Jupyter |
| Lessons | [Spark fundamentals](../unit4/fundamentals.md) | [Run it on workers: the job, its graph and parallelism](../beam/workers.md) |

A lab manager switches a unit on or off: `make status` on a laptop, **Manage → ⚙️ Settings → Services**
on a team cluster. The Flink unit is off until a class needs it.

## Spark UI
The Spark UI opens on the Spark master's page:

- the **Alive Workers** and their cores and memory;
- under **Running Applications**: **Spark Connect server**. Every notebook and every `lab_spark()` task
  runs inside it, so they don't show as applications of their own.
- Click **Spark Connect server** for its **Application UI**: the **Jobs**, **Stages**, **SQL** and
  **Executors** tabs show what Spark did for each query: how many tasks, where the time went.

The server is shared, so you see the whole class's queries there, not only yours. If your query waits,
others are using the cores; it goes on once they're free.

## Flink UI
The Flink UI is **read only**: it shows everything, and changes nothing (no one can cancel another
member's job). It shows:

- **Overview**: the TaskManagers, the slots (total and free), the running jobs;
- **Jobs**: each Beam pipeline is a job named **`<you>-<name>`** (the `name` you gave `lab_beam`). Click
  one for its graph: the steps as boxes, with how much data flows between them;
- **Task Managers**: each worker, its slots and memory.

From a notebook:

```python
p = beam.Pipeline(options=lab_beam("flink", name="channels", parallelism=4))   # 4 parallel copies
lab_beam_jobs()              # your running jobs: [(name, state, seconds)]
lab_beam_stop("orders-per-minute")   # stop your streaming job of that name
```

A streaming job runs until you stop it, and holds its slots meanwhile: stop it with `lab_beam_stop`.
If the Flink unit is off, `lab_beam("flink")` says so; `lab_beam("local")` works without it.

## The Flink cluster's size
Lab managers set it on **Manage → ⚙️ Settings → Flink cluster**: **TaskManagers** (1–4) × **slots
each** (1–4). The two multiplied is how many parallel copies of a step a job can run, across all the
jobs running at once. More slots also take more memory. A new size applies at once on a team cluster
(the TaskManagers restart, with the jobs on them); on a laptop with `make status` → Flink → **s**. See
[⚙️ Settings](settings.md).

## Related lessons
- [How Spark runs under the hood (architecture & the query lifecycle)](../unit4/spark-architecture.md)
- [Spark performance & tuning](../unit4/performance.md)
- [The development workflow: notebook → job → DAG](../unit5/dev-workflow.md)
- [Your first pipeline: PCollections and transforms](../beam/first-pipeline.md)
- [Run it on workers: the job, its graph and parallelism](../beam/workers.md)
- [A streaming pipeline: Pub/Sub topics on workers](../beam/kafka.md)
