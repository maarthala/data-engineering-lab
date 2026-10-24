# Engineering practices: Python and SQL quality, testing, observability, AI assistants

> **Production-grade, tested, maintainable** Python and SQL pipelines; Terraform, CI/CD and git; **AI coding assistants**; **metrics** for load duration, throughput, stability and cost; **testing, documentation and handover** of patterns.

---

## 1. What "production-grade pipeline" means (checklist)

| Property | Concretely |
|---|---|
| **Idempotent** | Rerun-safe: partition overwrite / MERGE, logical dates |
| **Tested** | Unit tests (logic), data tests (quality), integration tests (end-to-end on a dev dataset) |
| **Observable** | Logs, metrics (duration, rows, bytes, freshness), alerts with owners |
| **Configurable** | No hard-coded projects or dates; config per environment |
| **Version-controlled** | Everything in git: SQL, DAGs, Terraform, LookML |
| **Reviewed and automated** | PRs, CI checks, automated deploy |
| **Documented** | README, data contracts, runbook ("what to do when this alert fires") |
| **Secure** | Service account per pipeline, secrets in Secret Manager, no keys in code |
| **Cost-aware** | Labels, guardrails, efficient SQL |
| **Recoverable** | Backfill procedure, time travel, replayable raw data |

---

## 2. Testing data pipelines: the pyramid

```
            ▲  fewer, slower, more realistic
           ╱ ╲   End-to-end: run the pipeline on a dev/CI dataset, reconcile outputs
          ╱───╲
         ╱     ╲  Data tests: dbt tests / Dataform assertions / quality checks on real data
        ╱───────╲
       ╱         ╲ Unit tests: pure Python functions, SQL logic on small fixtures, DAG integrity
      ╱───────────╲ many, fast, cheap
```

### Unit tests: Python

```python
# src/transforms.py
from decimal import Decimal

def net_amount(gross: Decimal, discount: Decimal, vat_rate: Decimal) -> Decimal:
    """Net sales excluding VAT after discount."""
    if gross < 0 or discount < 0:
        raise ValueError("amounts must be non-negative")
    return ((gross - discount) / (1 + vat_rate)).quantize(Decimal("0.01"))
```

```python
# tests/test_transforms.py
from decimal import Decimal
import pytest
from src.transforms import net_amount

@pytest.mark.parametrize("gross,discount,vat,expected", [
    (Decimal("125.50"), Decimal("0"),     Decimal("0.255"), Decimal("100.00")),   # Finnish general VAT 25.5%
    (Decimal("125.50"), Decimal("25.10"), Decimal("0.255"), Decimal("80.00")),
])
def test_net_amount(gross, discount, vat, expected):
    assert net_amount(gross, discount, vat) == expected

def test_negative_rejected():
    with pytest.raises(ValueError):
        net_amount(Decimal("-1"), Decimal("0"), Decimal("0.255"))
```

### Unit tests: SQL logic

- **dbt unit tests** (dbt ≥ 1.8): give mocked input rows and expected output rows for a model.
- **Dataform:** `type: "test"` with input fixtures.
- Or a pytest harness that runs SQL against small **temp tables** in a CI dataset.

```yaml
# dbt unit test
unit_tests:
  - name: dedup_keeps_latest_version
    model: stg_pos__order_lines
    given:
      - input: source('raw_pos', 'order_lines')
        rows:
          - {order_id: "A", line_no: 1, qty: 1, _source_ts: "2026-10-22 10:00:00"}
          - {order_id: "A", line_no: 1, qty: 3, _source_ts: "2026-10-22 10:05:00"}
    expect:
      rows:
        - {order_id: "A", line_no: 1, qty: 3}
```

### Data tests (on real data, every run)

| Test | Catches |
|---|---|
| `unique` on the grain key | Duplicates from non-idempotent loads |
| `not_null` on keys | Broken joins, bad source data |
| `relationships` | Orphan facts (missing dimension rows) |
| `accepted_values` | New unexpected codes from the source |
| **Freshness** | Stalled ingestion |
| **Volume anomaly** | Partial loads (today 40% below the normal weekday) |
| **Reconciliation** with the source | Silent data loss |

**Blocking vs. warning:** critical tests (uniqueness, reconciliation of finance numbers) **stop** the pipeline before bad data reaches Looker. Others **alert**.

### Data contracts (upstream protection)

- An agreement with source teams: schema, types, semantics, SLAs, change process.
- Enforced with **dbt model contracts** (`contract: {enforced: true}`), schema checks in ingestion, and alerts on Datastream schema drift.
- Most pipeline failures come from upstream changes. Data contracts and schema-drift alerts turn surprises into planned changes.

---

## 3. Python code quality for data engineering

| Practice | Tool |
|---|---|
| Formatting and linting | **ruff** (lint + format) or black + flake8 |
| Type hints + checking | **mypy** / pyright |
| SQL linting | **sqlfluff** (BigQuery dialect) |
| Dependency management | `uv` / poetry / pip-tools with **pinned** versions |
| Packaging shared code | An internal package (e.g. `dataplatform-common`) used by DAGs and Dataflow |
| Pre-commit hooks | `pre-commit` running ruff, sqlfluff, terraform fmt |
| Secrets | **Secret Manager**, never in code or Airflow Variables in plain text |
| Logging | Structured (JSON) logs with pipeline, run_id, rows, duration fields |
| Config | Per-environment YAML or env vars; no hard-coded project IDs |

### Structure of a pipeline repo

```
data-platform/
 ├─ dags/                    # Airflow DAGs + factory + configs/
 ├─ dataform/ or dbt/        # SQL models, tests
 ├─ dataflow/                # Beam pipelines + Flex template Dockerfiles
 ├─ src/common/              # shared Python (clients, logging, validation)
 ├─ terraform/               # modules + envs
 ├─ tests/                   # pytest (unit, DAG integrity)
 ├─ .github/workflows/ or cloudbuild.yaml
 ├─ pyproject.toml
 └─ README.md + docs/        # architecture, runbooks, onboarding
```

---

## 4. Git and CI/CD practices

- **Trunk-based development** with short-lived feature branches and PRs. Main is always deployable.
- **PR template:** what changed, how it was tested, data impact (backfill needed?), rollback.
- **CODEOWNERS** per area (platform owns `terraform/`, domains own their models).
- **Environments:** dev → test → prod. Promote via merge and approval, not manual copying.
- **Data-specific CI:** BigQuery **dry runs**, dbt/Dataform build in a **PR-specific dataset** (e.g. `ci_pr_123`) with sampled or cloned data (**table clones** are cheap), then drop it after.
- **Deploy order** for breaking changes: expand → migrate → contract (add the new column, backfill, switch consumers, then drop the old one).

---

## 5. Metrics: load duration, throughput, stability, cost

Define them as **SLIs** (what you measure) with **SLOs** (targets):

| Category | SLI | Example SLO | Source |
|---|---|---|---|
| **Load duration** | Nightly chain end-to-end time; per-DAG p50/p95 duration | Nightly chain done by 06:00 on 99% of days | Airflow metadata DB |
| **Throughput** | Rows/sec or GB/hour per pipeline; Datastream events/sec | ≥ X rows/s at peak | Job stats, Datastream metrics |
| **Freshness** | `now - MAX(_source_ts)` per mart | Sales mart ≤ 15 min behind, 99% of the time | Scheduled query → metrics table |
| **Stability** | Task failure rate, retries, **reruns needed**, incidents per month | < 1% failed task runs; 0 manual reruns/week | Airflow metadata, incident log |
| **Data quality** | % of tests passing, reconciliation differences | 100% of blocking tests pass | dbt/Dataform results |
| **Cost** | BigQuery slot-hours / TB billed per pipeline per day; cost per 1M rows loaded | −30% vs. baseline | `INFORMATION_SCHEMA.JOBS` + labels + billing export |

### How to implement it

1. **Labels on everything** (`pipeline`, `domain`, `layer`, `env`) for BigQuery jobs, Dataflow jobs and resources.
2. **One ops dataset** (`ops_metrics`) with scheduled queries that collect:
    - Airflow run stats (from the Composer metadata DB export or the Airflow REST API)
    - `INFORMATION_SCHEMA.JOBS` aggregated by label
    - Freshness per mart
    - Billing export
3. **A Looker dashboard** "Platform health": trends before and after modernization. That's the **proof** for the business.
4. **Alerts** in Cloud Monitoring on SLO breaches, routed to the owning team.

Baseline these metrics in the first week, so every change can be shown as a before/after number: runtime, failure rate, freshness and cost.

---

## 6. Documentation and handover

| Artifact | Content |
|---|---|
| **Architecture overview** | Diagram: sources → ingestion → layers → Looker; projects and IAM |
| **Pattern docs** | "How to onboard a source", "how to build an incremental model", "how to backfill" |
| **Runbooks** | Per alert: meaning, first checks, fixes, escalation |
| **Data catalog** | dbt docs / Dataform descriptions / Dataplex: table and column descriptions, owners |
| **Decision log (ADRs)** | Why Dataform over dbt, why merge mode, why these partitions |

Handover approach: **show → pair → review → own.** The internal team does wave 2 with review support and wave 3 alone.

---

## 7. AI coding assistants

Practical and responsible use:

- **Where they fit:** boilerplate (Terraform resources, DAG skeletons, test cases), SQL translation drafts (e.g. Spark SQL → BigQuery), regex and parsing, documentation drafts, explaining unfamiliar code, generating test data.
- **How to keep quality:**
    - Treat the output as a **junior colleague's PR**: review, test, understand every line
    - Tests and CI catch regressions regardless of who wrote the code
    - **Dry-run** generated SQL and check the bytes scanned and the query plan
    - Never paste secrets, credentials or customer PII into prompts; follow the company's AI policy
- **Where they help most in an EDW modernization:** migrating 300 DAGs to a factory pattern, translating SQL dialects, writing dbt/Dataform tests at scale, writing descriptions for the semantic layer (then reviewing them).

In short: AI assistants speed up boilerplate, tests and SQL translation, but everything goes through the same review and CI as human code. The productivity gain is real, but accountability stays with the engineer.

---
