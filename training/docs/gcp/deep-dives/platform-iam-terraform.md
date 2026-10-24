# GCP platform: projects, IAM, networking, Terraform, CI/CD

> Moving the load layer into shared GCP ingestion projects: project layout, IAM, networking, reusable Terraform modules and CI/CD. Use correct IAM terms (**principal + role + resource**), and don't give the CI service account unlimited power.

---

## 1. Resource hierarchy

```
Organization (example.com)
 ├─ Folder: platform
 │    ├─ Project: prj-net-host            (Shared VPC host)
 │    ├─ Project: prj-ops-logging         (central logs, billing export)
 │    └─ Project: prj-tf-state            (Terraform state buckets, deployer SAs)
 ├─ Folder: prod
 │    ├─ Project: prj-ingest-prod         ← shared ingestion: Datastream, Pub/Sub, Dataflow, landing GCS, raw_* datasets
 │    ├─ Project: prj-orch-prod           ← Composer
 │    ├─ Project: prj-dwh-sales-prod      ← stg_sales, mart_sales (domain-owned)
 │    ├─ Project: prj-dwh-finance-prod
 │    └─ Project: prj-bi-prod             ← Looker connection SA, BI reservations
 ├─ Folder: nonprod (test, dev)
 │    └─ ... same layout
 └─ Folder: sandbox (personal experiments, budget-capped)
```

- **IAM and org policies inherit downward.** Grant at the **lowest level that works**.
- **A project is the boundary** for billing, quotas, APIs and many IAM roles. **A dataset** is the boundary for BigQuery data access.
- **Why separate projects per layer or domain:** blast radius, clear cost attribution, independent quotas, simple IAM ("the sales team owns `prj-dwh-sales-*`").
- **Why a shared ingestion project:** one place for connectivity to sources (VPN/Interconnect, private connectivity for Datastream), standard patterns, central monitoring, and no duplicated ingestion per domain.

### Datasets inside the ingestion project

```
prj-ingest-prod
 ├─ raw_pos          (Datastream: POS DB)
 ├─ raw_ecom         (Datastream: e-commerce DB)
 ├─ raw_erp_files    (batch files from GCS)
 └─ ops_ingestion    (audit tables, load logs, freshness metrics)
```

One dataset **per source** gives clean IAM ("sales can read `raw_pos`") and clear ownership.

Cross-project reads are normal in BigQuery: `SELECT * FROM prj-ingest-prod.raw_pos.orders`. The query **runs and is billed** in the project where the job runs (it needs `bigquery.jobUser` there), and the data is read under the dataset's IAM.

---

## 2. IAM done right

### Terms

| Term | Meaning | Example |
|---|---|---|
| **Principal** | Who | `group:sales-analysts@example.com`, `serviceAccount:sa-ingest-pos@prj-ingest-prod.iam.gserviceaccount.com` |
| **Role** | Set of permissions | `roles/bigquery.dataViewer` |
| **Resource** | Where | Organization, folder, project, **dataset**, table, bucket, topic |
| **Binding** | Principal + role on a resource | "sales-analysts have dataViewer on dataset mart_sales" |
| **Policy** | All bindings on one resource | |

### Rules

1. **Humans go into Google Groups** (from the identity provider). Never bind individual users.
2. **Workloads get service accounts**: **one per pipeline or source** (`sa-ingest-pos`, `sa-dataform-sales`, `sa-looker`), so the blast radius stays small and audit logs are readable.
3. **Least privilege:** the **smallest role** at the **lowest resource level**.
4. **Predefined roles first.** **Custom roles** only when predefined roles are too broad.
5. **No basic roles** (`Owner`, `Editor`, `Viewer`) in prod. They are far too broad.
6. **Impersonation instead of keys:** humans or CI use `roles/iam.serviceAccountTokenCreator` to act as a service account. **No downloaded JSON keys** (an org policy can block key creation).

### BigQuery roles to know

| Role | Grants | Typical binding |
|---|---|---|
| `roles/bigquery.dataViewer` | Read tables and views | Group → dataset |
| `roles/bigquery.dataEditor` | Read and write tables in the dataset | Pipeline SA → its own dataset |
| `roles/bigquery.dataOwner` | + manage dataset IAM | Platform team only |
| `roles/bigquery.jobUser` | **Run jobs** (queries) in a project | Group/SA → the project where they run queries |
| `roles/bigquery.user` | jobUser + create datasets | Avoid in prod |
| `roles/bigquery.readSessionUser` | Storage Read API (Spark, fast exports) | Dataproc SA |
| `roles/bigquery.admin` | Everything | Break-glass only |

**Read access needs two things:** `dataViewer` on the **dataset** + `jobUser` on **some project** to run the query. This is a common source of confusion.

### Example: least-privilege bindings for the sales domain

| Principal | Role | Resource |
|---|---|---|
| `sa-ingest-pos` | `datastream`-related roles, `bigquery.dataEditor` | dataset `raw_pos` only |
| `sa-dataform-sales` | `bigquery.dataViewer` | `raw_pos`, `raw_ecom` |
| `sa-dataform-sales` | `bigquery.dataEditor` | `stg_sales`, `mart_sales` |
| `sa-dataform-sales` | `bigquery.jobUser` | `prj-dwh-sales-prod` |
| `group:sales-engineers` | `bigquery.dataViewer` | `raw_pos`, `stg_sales`; dataEditor in **dev** only |
| `group:sales-analysts` | `bigquery.dataViewer` | `mart_sales` only |
| `sa-looker` | `bigquery.dataViewer` on marts, `jobUser` on `prj-bi-prod` | |

### Fine-grained data security (follow-up)

- **Column level:** policy tags + `Fine-Grained Reader`, plus **dynamic masking**
- **Row level:** row access policies
- **Authorized views / datasets:** share results without base-table access
- **VPC Service Controls:** a **perimeter** around projects to stop data exfiltration (for example copying a table to a personal project). Important for sensitive data.

---

## 3. Organization policies (guardrails)

| Constraint | Purpose |
|---|---|
| `gcp.resourceLocations` → `in:eu-locations` (or `europe-north1`) | **Data residency.** A Finnish client probably needs EU |
| `iam.disableServiceAccountKeyCreation` | No JSON keys |
| `iam.allowedPolicyMemberDomains` | Only identities from your domain can be granted access |
| `storage.publicAccessPrevention` | No public buckets |
| `storage.uniformBucketLevelAccess` | IAM only, no ACLs |
| `compute.vmExternalIpAccess` | No public IPs on VMs (Dataflow workers private) |

---

## 4. Networking (just enough)

- **Shared VPC:** networks live in a **host project** and service projects (ingestion, Composer) attach to them. Central control of firewalls and routes.
- **Private Google Access:** private VMs and Dataflow workers reach Google APIs without public IPs.
- **Reaching source databases:**
    - On-prem → **Cloud VPN / Interconnect** to the VPC
    - Datastream → **private connectivity** (VPC peering or Private Service Connect), or IP allowlisting, or a forward SSH tunnel
- **Composer** with private IP and **Dataflow** with `--no_use_public_ips`.

---

## 5. Terraform

### Repository layout

```
terraform/
 ├─ modules/
 │   ├─ project/                # project + APIs + labels + billing + budget
 │   ├─ ingestion_source/       # "onboard a new source"
 │   ├─ bq_dataset/             # dataset + IAM + defaults
 │   └─ composer_env/
 ├─ envs/
 │   ├─ dev/   main.tf  backend.tf  terraform.tfvars
 │   ├─ test/
 │   └─ prod/
 └─ policies/                   # OPA/conftest or checkov rules
```

- **Remote state** in a **GCS bucket per environment** (versioning on, locking built in):
    ```hcl
    terraform {
      backend "gcs" {
        bucket = "tfstate-prod-example"
        prefix = "ingestion"
      }
    }
    ```

- **Pin versions:** `required_version`, provider versions, and **module versions** (git tags or a registry).
- Google's community modules (`terraform-google-modules/*`) and the **Cloud Foundation Fabric** blueprints are good references.

### The `ingestion_source` module

```hcl
# modules/ingestion_source/variables.tf
variable "source_name"     { type = string }   # "pos"
variable "source_type"     { type = string }   # "postgres" | "mysql" | "gcs_files"
variable "env"             { type = string }
variable "project_id"      { type = string }
variable "owner_domain"    { type = string }   # "sales"
variable "composer_bucket" { type = string }

variable "location" {
  type    = string
  default = "EU"
}
variable "reader_groups" {                     # groups that may read raw
  type    = list(string)
  default = []
}
variable "freshness_sla_minutes" {
  type    = number
  default = 15
}
variable "contains_pii" {
  type    = bool
  default = false
}
variable "tables" {                            # for CDC sources
  type    = list(string)
  default = []
}
```

```hcl
# modules/ingestion_source/main.tf (simplified)
locals {
  labels = { source = var.source_name, owner = var.owner_domain, env = var.env, pii = tostring(var.contains_pii) }
}

resource "google_service_account" "ingest" {
  project      = var.project_id
  account_id   = "sa-ingest-${var.source_name}"
  display_name = "Ingestion SA for ${var.source_name}"
}

resource "google_storage_bucket" "landing" {
  project                     = var.project_id
  name                        = "${var.project_id}-landing-${var.source_name}"
  location                    = var.location
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  labels                      = local.labels
  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }
}

resource "google_bigquery_dataset" "raw" {
  project     = var.project_id
  dataset_id  = "raw_${var.source_name}"
  location    = var.location
  labels      = local.labels
  description = "Raw data from ${var.source_name}, owned by ${var.owner_domain}"
}

# Writer: only this source's SA can write to this dataset
resource "google_bigquery_dataset_iam_member" "writer" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.raw.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.ingest.email}"
}

# Readers: domain groups
resource "google_bigquery_dataset_iam_member" "readers" {
  for_each   = toset(var.reader_groups)
  project    = var.project_id
  dataset_id = google_bigquery_dataset.raw.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "group:${each.value}"
}

# CDC sources: Datastream connection profile + stream (credentials from Secret Manager)
# resource "google_datastream_connection_profile" "source" { count = var.source_type == "postgres" ? 1 : 0 ... }
# resource "google_datastream_stream" "this" { ... destination_config { bigquery_destination_config { ... } } }

# Monitoring: alert when freshness breaches the SLA
# resource "google_monitoring_alert_policy" "freshness" { ... }

# DAG-factory config for Composer, generated from the same inputs
resource "google_storage_bucket_object" "dag_config" {
  bucket  = var.composer_bucket
  name    = "dags/configs/sources/${var.source_name}.yaml"
  content = yamlencode({ source = var.source_name, type = var.source_type, owner = var.owner_domain,
                         target_dataset = google_bigquery_dataset.raw.dataset_id,
                         freshness_sla_minutes = var.freshness_sla_minutes })
}
```

```hcl
# modules/ingestion_source/outputs.tf
output "dataset_id"             { value = google_bigquery_dataset.raw.dataset_id }
output "service_account_email"  { value = google_service_account.ingest.email }
output "landing_bucket"         { value = google_storage_bucket.landing.name }
```

A **team onboards a source** with one block in a PR:

```hcl
module "src_pos" {
  source        = "git::https://github.com/example/tf-modules.git//ingestion_source?ref=v1.4.0"
  source_name   = "pos"
  source_type   = "postgres"
  env           = "prod"
  project_id    = "prj-ingest-prod"
  owner_domain  = "sales"
  reader_groups = ["sales-engineers@example.com"]
  contains_pii  = true
  tables        = ["public.orders", "public.order_lines"]
}
```

### ⚠️ IAM resource types in Terraform (a classic trap)

| Resource | Behaviour | Risk |
|---|---|---|
| `google_*_iam_member` | **Additive**: adds one member to one role | Safe, the default choice |
| `google_*_iam_binding` | **Authoritative for one role**: sets *all* members of that role | Removes members added elsewhere |
| `google_*_iam_policy` | **Authoritative for the whole policy** on the resource | Can lock everyone out, including Google service agents |

Rule of thumb: use `iam_member` by default. Use authoritative bindings only where Terraform is the single owner of that role.

### Other Terraform practices

- `terraform fmt`, `validate`, **tflint**, **checkov / tfsec / trivy** (security scanning), OPA/conftest for custom policies ("no basic roles", "labels required", "location must be EU").
- `prevent_destroy` lifecycle on critical datasets and buckets.
- Importing existing resources (`import` blocks) when bringing a hand-made platform under IaC, which is likely in this modernization.
- Use `moved` blocks to refactor without destroying resources.
- **Drift detection:** a scheduled `terraform plan` that alerts when it's not empty.

---

## 6. CI/CD for infrastructure and data code

```
Developer → PR
  ├─ terraform fmt -check, validate, tflint, checkov, conftest
  ├─ terraform plan (dev/test/prod) → plan posted as PR comment
  ├─ CODEOWNERS: platform team must approve changes to IAM, networking, prod
  └─ merge → main
        ├─ apply dev (auto)
        ├─ apply test (auto)
        └─ apply prod (manual approval / environment protection)
```

- **The CI identity:** one **deployer service account per environment** (`sa-tf-dev`, `sa-tf-prod`). The dev deployer **can't** touch prod.
- **Workload Identity Federation:** GitHub Actions/GitLab/Cloud Build gets short-lived tokens via OIDC, so **no keys are stored in CI**.
- The deployer service account has broad rights on its environment's folder, so **protect it**: only the main branch can impersonate it, changes are reviewed, and audit logs are on.
- **Plan on PR, apply on merge.** Never apply from a laptop to prod.

```yaml
# .github/workflows/terraform.yml (essentials)
permissions: { id-token: write, contents: read, pull-requests: write }
jobs:
  plan:
    steps:
      - uses: actions/checkout@v4
      - uses: google-github-actions/auth@v2
        with:
          workload_identity_provider: projects/123/locations/global/workloadIdentityPools/gh/providers/github
          service_account: sa-tf-prod@prj-tf-state.iam.gserviceaccount.com
      - uses: hashicorp/setup-terraform@v3
      - run: terraform -chdir=envs/prod init && terraform -chdir=envs/prod plan -out=plan.tfplan
```

---

## 7. Adoption: how teams use it on their own

1. **A template repo** with a working example source (copy, rename, PR).
2. **Docs:** a README per module (inputs, outputs, examples) and a "how to onboard a source in 30 minutes" guide.
3. **Guardrails in CI**, so mistakes are caught by tools, not reviewers.
4. **Versioned modules + changelog**, so teams upgrade deliberately.
5. **Golden path:** the easy way is the right way (standard naming, labels, alerts automatically).
6. **Handover:** pairing sessions, office hours, and the internal team owning the module after N weeks.

---

## 8. AWS / Azure → GCP mapping for this topic

| AWS | Azure | GCP |
|---|---|---|
| Organization / OU | Management group | Organization / **Folder** |
| Account | Subscription (+ resource group) | **Project** |
| IAM role (for a service) | Managed identity | **Service account** |
| IAM policy attachment | Role assignment | **IAM binding** |
| SCP | Azure Policy | **Organization policy** |
| VPC peering / Transit GW | VNet peering / vWAN | VPC peering / **Shared VPC** |
| PrivateLink | Private endpoint | **Private Service Connect** |
| GitHub OIDC → IAM role | Federated credentials | **Workload Identity Federation** |
| S3 backend + DynamoDB lock | Azure Storage backend | **GCS backend** (locking built in) |

---
