# The GCP platform: resource hierarchy, IAM, networking, Terraform and CI/CD

> **How to use this page.** Read it once from top to bottom. It teaches from the fundamentals. For a short revision sheet, use the [platform, IAM and Terraform deep dive](../deep-dives/platform-iam-terraform.md).
>
> **Three key ideas:**
>
> 1. Start every platform design with the **layout** (folders, projects, datasets).
> 2. Use the correct IAM words: a **principal** gets a **role** on a **resource**. That is a **binding**.
> 3. A CI service account never gets "all privileges". The safe pattern is **one deployer per environment, Workload Identity Federation, plan on PR, apply on merge, CODEOWNERS**.

---

## 1. What it is and why it exists

A data platform is more than pipelines. Somebody must decide:

- **Where** things live (which project, which region).
- **Who** can do **what** to them (IAM).
- **What is never allowed**, even for admins (organization policies, VPC Service Controls).
- **How** things are created and changed in a safe, repeatable way (Terraform and CI/CD).

On GCP these answers are built from a few building blocks: the **resource hierarchy** (organization → folders → projects → resources), **IAM** (allow policies with bindings), **guardrails** (org policies, VPC-SC), **networking**, and **infrastructure as code**.

### An analogy: an office building

| Building | GCP |
|---|---|
| The company that owns the building | **Organization** (your domain, e.g. `example.com`) |
| Floors (Sales floor, Finance floor) | **Folders** |
| Rooms, each with its own electricity meter and lock | **Projects** (own billing, quotas, APIs, IAM) |
| Cabinets and drawers in a room | **Resources** (datasets, buckets, topics) |
| Key cards: "Anna may open cabinet X" | **IAM bindings** |
| A key card that works on the whole floor | A binding on a **folder** (inherited by every room) |
| Building rules: "no open flames, ever" | **Organization policies** |
| Guards at the doors who stop boxes leaving | **VPC Service Controls** |
| The architect's drawings, used to build every new floor the same way | **Terraform modules** |
| A change process: drawings are reviewed before builders act | **CI/CD with plan on PR, apply on merge** |

The important idea: **rules flow downward**. A key card for the floor opens every room on the floor. That is powerful and dangerous, so you give out floor-level cards very rarely.

---

## 2. Where it fits in a modern GCP data platform

A typical enterprise data warehouse (EDW) modernization on GCP moves the load layer into **shared ingestion projects**, with reusable ingestion patterns, a clear project structure, IAM and Terraform modules. Terraform, CI/CD and git are the everyday tools.

That means, in practice:

1. **Design the project structure** for a shared ingestion platform that many domain teams use (sales, finance, supply chain).
2. **Design identities and permissions**: which service account runs which pipeline, which group can read which dataset, with least privilege.
3. **Write Terraform modules** so that "onboard a new source" is one small pull request, not two weeks of clicking.
4. **Run Terraform through CI/CD** in a safe way (no keys, reviewed plans, separate environments).
5. **Hand it over**: other teams must be able to use the modules without the platform team.

Existing platforms are often built by hand, project by project. So expect **brownfield** work: importing existing resources into Terraform, changing IAM without breaking dashboards, and moving ingestion without breaking consumers (the migration side is in [Migration and modernization](migration-modernization.md)).

---

## 3. Architecture: how it works under the hood

### 3.1 The resource hierarchy

```text
Organization: example.com  (org ID 123456789012)
│   org policies: EU-only locations, no SA keys, domain-restricted sharing
│
├── Folder: platform
│     ├── Project: prj-net-host-prod      (Shared VPC host: networks, firewalls, VPN)
│     ├── Project: prj-logging            (central log buckets, billing export)
│     └── Project: prj-iac                (Terraform state buckets, deployer SAs, WIF pool)
│
├── Folder: prod
│     ├── Project: prj-ingest-prod        ← shared ingestion (Datastream, Pub/Sub, Dataflow,
│     │                                      landing buckets, raw_* datasets)
│     ├── Project: prj-orch-prod          ← Cloud Composer
│     ├── Project: prj-dwh-sales-prod     ← stg_sales, mart_sales
│     ├── Project: prj-dwh-finance-prod
│     └── Project: prj-bi-prod            ← Looker connection, BI reservation assignment
│
├── Folder: nonprod
│     ├── Folder: test   (same projects, suffix -test)
│     └── Folder: dev    (same projects, suffix -dev)
│
└── Folder: sandbox                       (personal experiments, budgets, auto-cleanup)
```

The **billing account** is not in this tree. It is a separate object, with its own IAM, that is **linked** to projects. One billing account can pay for many projects.

### 3.2 What happens when a pipeline makes an API call

Every action in GCP is an API call. When a Dataflow job writes to BigQuery, this happens:

```text
 (1) Who are you?            (2) Are you allowed?                 (3) Is it allowed at all?
 ┌──────────────┐  OAuth   ┌──────────────────────────────────┐  ┌───────────────────────────┐
 │ Dataflow     │  access  │ IAM check on the target resource │  │ Org policy (at create     │
 │ worker runs  │  token   │  - deny policies first           │  │   time: e.g. location)    │
 │ as SA        ├────────► │  - allow policy of the resource  ├─►│ VPC-SC perimeter          │
 │ sa-ingest-pos│          │    + all its ancestors (union)   │  │   (is the call crossing   │
 └──────────────┘          │  - conditions evaluated          │  │    the perimeter?)        │
                           └──────────────────────────────────┘  └─────────────┬─────────────┘
                                                                               ▼
                                                      bigquery.googleapis.com writes the rows
                                                      → Cloud Audit Logs record the call
```

Three independent layers:

1. **Authentication**: the caller proves who it is with a short-lived OAuth 2.0 access token.
2. **Authorization (IAM)**: does any binding give this principal the needed **permission** on this resource or one of its parents? Is there a **deny** policy that blocks it?
3. **Guardrails**: org policies restrict *what can be configured* (for example "datasets only in the EU"). VPC-SC restricts *where data can flow* (for example "no copy to a project outside the perimeter"). They apply **even to Owners**.

### 3.3 How Terraform fits in

```text
 Git repo (HCL)                  CI runner (GitHub Actions / Cloud Build)
 ┌─────────────┐   PR      ┌─────────────────────────────────────────────┐
 │ modules/    │ ───────►  │ fmt, validate, tflint, checkov, conftest    │
 │ envs/prod/  │           │ terraform plan  ──► comment on the PR       │
 └─────────────┘           │   (merge)                                   │
                           │ terraform apply ──► GCP APIs                │
                           └───────┬───────────────────────┬─────────────┘
                     OIDC token    │                       │ reads/writes state + lock
                     → WIF → SA    ▼                       ▼
                    ┌─────────────────────┐     ┌─────────────────────────┐
                    │ sa-tf-prod          │     │ gs://example-tfstate-prod│
                    │ (deployer, prod     │     │ versioned, locked down  │
                    │  folder only)       │     └─────────────────────────┘
                    └─────────────────────┘
```

---

## 4. Core concepts, one by one

Each concept follows the same pattern: **what → how it works → why it matters → example → common mistake**.

### 4.1 Organization and folders

- **What.** The **organization** is the root node. It is tied to one Google Workspace or Cloud Identity domain. **Folders** group projects and other folders. Folders can nest (up to 10 levels).
- **How it works.** IAM allow policies and org policies can be attached to the org, to any folder, or to a project. Everything below **inherits** them. A project has exactly one parent.
- **Why it matters.** Folders are the place for **environment-wide** and **team-wide** rules: "the platform team is admin on the `prod` folder", "dev may use any region in the EU". You set a rule once and every new project gets it.
- **Example.** Folder `prod` gets the binding `group:platform-admins → roles/resourcemanager.folderViewer`, and an org policy that blocks public IPs.
- **Common mistake.** Granting roles on the **organization** "because it's easier". An org-level `roles/bigquery.dataViewer` means that group can read **every** dataset in the company, now and in the future.

### 4.2 What a project really is

- **What.** A project is the basic container for resources. It has a **project ID** (globally unique, chosen by you, immutable, e.g. `prj-ingest-prod`), a **project number** (assigned by Google), and a display name.
- **How it works.** A project is at the same time:
    - an **API boundary**: each service (BigQuery, Dataflow, Datastream) must be **enabled** in the project before you can use it;
    - a **billing boundary**: the project is linked to one billing account, and costs are reported per project;
    - a **quota boundary**: most quotas are per project (and often per region);
    - an **IAM boundary**: a project-level binding applies to all resources in the project;
    - a home for **service agents** (Google-managed identities, see 4.9) and default service accounts.

    When you delete a project, it is first **shut down** for about 30 days and can be restored in that period.
- **Why it matters.** Separate projects per layer or domain give you **blast-radius control** (a broken pipeline can't burn the finance quota), **clean cost attribution**, and **simple IAM** ("sales owns `prj-dwh-sales-*`").
- **Example.** The shared ingestion project enables `datastream.googleapis.com`, `pubsub.googleapis.com`, `dataflow.googleapis.com`, `bigquery.googleapis.com`, `storage.googleapis.com`, `secretmanager.googleapis.com`. A mart project needs only `bigquery.googleapis.com`.
- **Common mistake.** One giant project for everything. Then IAM becomes dataset-by-dataset exceptions, costs can't be split, and one team's runaway query uses everyone's quota. The opposite mistake is a project per table: too many projects to manage.

### 4.3 Regions, zones, multi-regions and BigQuery locations

- **What.** A **region** is a geographic area (e.g. `europe-north1`, which is in Hamina, Finland). A **zone** is an isolated location inside a region (`europe-north1-a`). A **multi-region** is a large area made of several regions (BigQuery and GCS have `EU` and `US`).
- **How it works.** Compute resources (Dataflow workers, Composer) run in a region and its zones. A **BigQuery dataset has a location** that you pick at creation, and it **can't be changed** later. A query **runs in the location of the data it reads**. In general, one query can't join tables from two different locations (for example `EU` and `europe-north1`). Check current docs, because Google has been adding cross-region features.
- **Why it matters.**
    - **Data residency.** European companies usually want data in the EU, sometimes in one specific country. You enforce this with the org policy `gcp.resourceLocations` and by choosing dataset locations on purpose.
    - **Consistency.** If raw data is in `EU` and marts are in `europe-north1`, transformations can't join them. **Pick one location for the whole warehouse** and put it in the Terraform module default.
    - **Cost and latency.** Moving data between regions costs network egress.
- **Example.** Standard: all BigQuery datasets in `EU` multi-region (or all in `europe-north1`), all GCS buckets in the same location, Dataflow and Composer in `europe-north1` or `europe-west1`.
- **Common mistake.** Creating a dataset with the default location (historically `US` in some tools) by accident. Then you must copy the data to a new dataset to fix it.

### 4.4 Principals: who can act

| Principal type | Example | Used for |
|---|---|---|
| Google account (user) | `user:anna@example.com` | A human. **Don't bind users directly.** |
| Google group | `group:sales-engineers@example.com` | Humans, managed in the identity provider (often synced from Entra ID / Okta) |
| Service account | `serviceAccount:sa-ingest-pos@prj-ingest-prod.iam.gserviceaccount.com` | A workload (pipeline, Composer, Dataflow, CI) |
| Workload identity (federated) | `principalSet://iam.googleapis.com/projects/NUM/locations/global/workloadIdentityPools/github/attribute.repository/example/infra` | An external workload (GitHub Actions) without a key |
| Workforce identity (federated) | users from an external IdP without Google accounts | Humans signing in with the company IdP |
| Domain / special | `domain:example.com`, `allUsers`, `allAuthenticatedUsers` | Avoid. `allUsers` = public. |

- **Why groups.** When Anna leaves, IT removes her from the group in the IdP and all her GCP access disappears. Nobody needs to search through hundreds of bindings. Groups also keep policies small (there are limits on policy size).
- **Common mistake.** Letting people log in **as** a shared service account to "share access". Then audit logs show the service account, not the person. Humans use **groups**; workloads use **service accounts**.

### 4.5 Roles and permissions

- **What.** A **permission** is one allowed action, written `service.resource.verb`, e.g. `bigquery.tables.getData`, `bigquery.jobs.create`, `storage.objects.create`. You never grant a permission directly. You grant a **role**, which is a named set of permissions.
- **Three kinds of roles.**

| Kind | Examples | Use |
|---|---|---|
| **Basic** | `roles/owner`, `roles/editor`, `roles/viewer` | Too broad. Editor can change almost everything in a project. **Not in prod.** |
| **Predefined** | `roles/bigquery.dataViewer`, `roles/pubsub.publisher`, `roles/dataflow.worker` | **Default choice.** Google maintains them and adds new permissions when services change. |
| **Custom** | `organizations/123/roles/ingestionOperator` | When a predefined role is too broad. Defined at **org or project** level (not folder). You must maintain it. |

- **Why it matters.** Least privilege means: the **smallest role** on the **lowest resource**. A predefined role on a dataset is usually better than a custom role on a project.
- **Example.** A pipeline that only publishes events needs `roles/pubsub.publisher` on **one topic**, not `roles/pubsub.editor` on the project.
- **Common mistake.** Making a custom role for every small case. Custom roles don't get new permissions automatically, so things break silently when a service adds a new permission. Use them only where it clearly matters (for example, a support role that can see job metadata but no data).

### 4.6 Allow policies, bindings, inheritance and the effective policy

- **What.** Every resource that supports IAM has one **allow policy**. A policy is a list of **bindings**. A binding says: *these principals have this role (optionally, under this condition)*.

```json
{
  "bindings": [
    {
      "role": "roles/bigquery.dataViewer",
      "members": [
        "group:sales-engineers@example.com",
        "serviceAccount:sa-dataform-sales@prj-dwh-sales-prod.iam.gserviceaccount.com"
      ]
    }
  ],
  "etag": "BwX...",
  "version": 1
}
```

- **How it works.**
    - The **effective policy** on a resource is the **union** of its own policy and the policies of all its ancestors (project, folders, organization).
    - Allow policies are **additive only**. A child can't remove a role granted at the parent. If the org grants `viewer` to a group, no project can "un-grant" it with an allow policy. (Only a **deny** policy can block it.)
    - The `etag` is for **optimistic concurrency**: you read the policy, change it, and write it back with the etag. If someone else changed it in between, the write fails. This is why "read-modify-write" tools exist, and why Terraform's authoritative resources are dangerous (see 4.19).
    - `version: 3` is needed when a policy contains conditions.
    - IAM changes are usually effective within a minute or two, but they are **eventually consistent**, so a pipeline right after a grant may fail once. Add retries.
- **Why it matters.** When someone asks "why can this group see the finance dataset?", the answer is often an inherited binding two folders up. Tools: **Policy Analyzer** ("who has access to what") and **Policy Troubleshooter** ("why was this request allowed or denied?").
- **Common mistake.** Thinking a dataset-level policy can restrict a project-level grant. It can't. If `sales-analysts` has `bigquery.dataViewer` on the project, they can read **every** dataset in it, whatever the dataset policy says.

### 4.7 IAM conditions

- **What.** A binding can have a **condition** written in CEL (Common Expression Language). The role applies only when the condition is true.
- **How it works.** Common attributes: `request.time` (temporary access), `resource.name` and `resource.type` (only some resources), and **resource tags** (`resource.matchTag(...)`). Not every service supports every attribute, so check the docs for the service.
- **Example: temporary access for an incident.**

```hcl
resource "google_project_iam_member" "oncall_temp" {
  project = "prj-ingest-prod"
  role    = "roles/dataflow.developer"
  member  = "group:ingest-oncall@example.com"

  condition {
    title       = "expires-2026-11-01"
    description = "Temporary access for incident INC-1234"
    expression  = "request.time < timestamp(\"2026-11-01T00:00:00Z\")"
  }
}
```

- **Why it matters.** It gives you **time-bound** and **attribute-based** access without separate projects. Conditions are also used to **limit what a deployer can grant** (for example "this SA may only grant these roles"; check the docs for "limit role grants with conditions").
- **Common mistake.** Using conditions as the main access model. They are harder to read and audit. Use a clean project/dataset structure first, conditions as an extra.

### 4.8 Deny policies (brief)

- **What.** A **deny policy** says "these principals can **never** use these permissions, even if an allow policy grants them". It can be attached to the org, a folder or a project. It can list **exception principals**.
- **How it works.** IAM checks deny policies **before** allow policies. Permissions in deny policies use a different format, e.g. `bigquery.googleapis.com/tables.delete`. Not all permissions are supported yet; check the docs.
- **Example.** "Nobody except `group:break-glass-admins` may delete BigQuery datasets in the `prod` folder."
- **Common mistake.** Using deny policies to fix a messy allow model. Fix the bindings first. Deny is for a few hard rules. (There are also **Principal Access Boundary** policies that limit which resources a principal can access at all; good to know the name.)

### 4.9 Service accounts in depth

A **service account (SA)** is an identity for a workload. It is two things at once:

1. A **principal**: it can receive roles ("`sa-ingest-pos` can write to `raw_pos`").
2. A **resource**: other principals can get roles **on** it ("Composer may impersonate `sa-ingest-pos`").

This double nature is the key to understanding SA security.

#### Kinds of service accounts

| Kind | Email format | Who manages it | Notes |
|---|---|---|---|
| **User-managed** | `NAME@PROJECT_ID.iam.gserviceaccount.com` | You | One per pipeline. This is what you design. |
| **Default SAs** | `PROJECT_NUMBER-compute@developer.gserviceaccount.com` (Compute Engine default) | Created by Google when an API is enabled, then yours | Historically got **Editor** on the project automatically. Dataflow workers use it if you don't set a worker SA. **Don't use it**; the org policy `iam.automaticIamGrantsForDefaultServiceAccounts` stops the automatic Editor grant. |
| **Service agents** | `service-PROJECT_NUMBER@gcp-sa-SERVICE.iam.gserviceaccount.com` | Google | The identity a Google **service** uses to act in your project (e.g. the Dataflow or Composer service agent). They hold roles like `roles/dataflow.serviceAgent`. **Never remove those bindings.** |

#### Keys vs. impersonation

- **Service account keys** are long-lived JSON private keys. Whoever has the file **is** the SA until the key is deleted. They leak into git repos, laptops and CI variables. **Avoid them.** Block them with the org policy `iam.disableServiceAccountKeyCreation` (newer organizations get this enforced by default; check docs).
- **Attached SAs**: a VM, Dataflow worker, Cloud Run service or Composer environment runs **as** an SA. The code gets tokens from the **metadata server** automatically. No key exists.
- **Impersonation**: a principal with `roles/iam.serviceAccountTokenCreator` **on the SA** can ask the IAM Credentials API for a **short-lived access token** of that SA (default lifetime 1 hour; up to 12 hours only if an org policy allows it). Audit logs show **both** the caller and the SA.

```bash
# A human engineer runs a command as the pipeline SA (for debugging in dev)
gcloud storage ls gs://prj-ingest-dev-landing-pos \
  --impersonate-service-account=sa-ingest-pos@prj-ingest-dev.iam.gserviceaccount.com

# Terraform can also impersonate (in the provider block):
#   provider "google" { impersonate_service_account = "sa-tf-dev@prj-iac.iam.gserviceaccount.com" }
```

#### The two roles people always confuse

| Role on the SA | Key permission | Meaning | Example |
|---|---|---|---|
| `roles/iam.serviceAccountUser` | `iam.serviceAccounts.actAs` | "I may **attach** this SA to a resource I create" | The SA that launches a Dataflow job needs `serviceAccountUser` on the **worker SA**, so the job can run as it. Same for creating a Composer environment or a Cloud Run service with a custom SA. |
| `roles/iam.serviceAccountTokenCreator` | `iam.serviceAccounts.getAccessToken` | "I may **get tokens** and act as this SA directly" | Composer's SA impersonates `sa-ingest-pos` (Airflow operators support `impersonation_chain`). A CI identity impersonates the deployer SA. |

**Why this matters (privilege escalation).** If someone can create a Dataflow job and has `actAs` on a powerful SA, they can run any code with that SA's permissions. So `actAs` on a powerful SA is **as powerful as the SA itself**. Grant both roles **on the specific SA**, never on the whole project (a project-level `serviceAccountUser` gives `actAs` on **every** SA in the project).

#### Design rule
- **One SA per pipeline or source** (`sa-ingest-pos`, `sa-ingest-ecom`, `sa-dataform-sales`, `sa-looker`). Small blast radius, readable audit logs, easy to revoke.
- Name SAs consistently; put purpose in the description; label owners.
- **Common mistake.** One `sa-data-pipelines` with `bigquery.admin` that every DAG uses. One bug or leak = full access to everything.

### 4.10 BigQuery IAM

BigQuery has **two different kinds** of permission that people mix up:

1. **Data permissions**: can I read or write this table? Granted on the **dataset** (or table/view), with roles like `dataViewer`.
2. **Job permissions**: can I **run a query job**? A query is a job, and the job runs **in a project**. That needs `bigquery.jobs.create`, from `roles/bigquery.jobUser`, on the project where the job runs. That project also **pays** for the query (on-demand bytes, or the reservation's slots).

**The rule:** to read a table in another project, a principal needs **`dataViewer` on the dataset** (where the data lives) **plus `jobUser` on some project** (where the query runs).

```text
 group:sales-analysts
   │  jobUser on prj-bi-prod ───────► the query job runs and is billed HERE
   │
   └─ dataViewer on dataset prj-dwh-sales-prod.mart_sales ──► data is read from HERE
        (storage is billed to prj-dwh-sales-prod)
```

| Role | What it gives | Typical binding |
|---|---|---|
| `roles/bigquery.dataViewer` | Read table data and metadata | Group or SA → dataset |
| `roles/bigquery.dataEditor` | + create, update, delete tables, insert data | Pipeline SA → its own dataset |
| `roles/bigquery.dataOwner` | + manage the dataset and its access | Platform team / Terraform only |
| `roles/bigquery.metadataViewer` | See schemas, no data | Catalog / support users |
| `roles/bigquery.jobUser` | Run jobs (queries, loads, copies) | Project where the work runs |
| `roles/bigquery.user` | jobUser + list and **create datasets** | Dev/sandbox only |
| `roles/bigquery.readSessionUser` | Use the Storage Read API (Spark, fast reads) | Dataproc / Dataflow SA |
| `roles/bigquery.connectionUser` | Use an external connection (BigLake, remote functions) | SA that queries BigLake |
| `roles/bigquery.admin` | Everything | Break-glass only |

Finer-grained controls:

- **Authorized views / authorized datasets**: a view in dataset B may read dataset A, and users only get access to B. The view runs with **its own authorization**, not the user's.
- **Column-level security**: **policy tags** in a taxonomy; readers need `Fine-Grained Reader` on the tag. Plus **dynamic data masking**.
- **Row-level security**: `CREATE ROW ACCESS POLICY ... GRANT TO (...) FILTER USING (...)`.

**Common mistake.** Giving analysts `roles/bigquery.user` on the prod data project so they "can query". They can now create datasets there. Give `jobUser` on a BI or analytics project and `dataViewer` on the marts.

### 4.11 Cloud Audit Logs

| Log type | What it records | On by default? |
|---|---|---|
| **Admin Activity** | Changes to configuration and metadata (create dataset, set IAM policy) | Always on, can't be turned off, no charge |
| **Data Access** | Reads of config/metadata and reads/writes of user data | **Off by default for most services** (can be expensive). BigQuery is the important exception: its data access logs are on by default |
| **System Event** | Google's own actions (e.g. live migration) | Always on |
| **Policy Denied** | Requests denied by VPC-SC or org policies | On |

- **How it works.** Logs go into **log buckets** per project: `_Required` (admin activity, about 400 days) and `_Default` (30 days by default). **Log sinks** route logs elsewhere. An **aggregated sink** at the org or folder level sends all projects' logs to a central logging project or a BigQuery dataset.
- **Why it matters.** "Who changed this IAM policy?" and "who read the PII table?" are answered here. For BigQuery you also have `INFORMATION_SCHEMA.JOBS` (who ran which query, bytes, slots, referenced tables).
- **Common mistake.** Enabling Data Access logs for **all** services at org level: you can get a big logging bill. Enable them where they matter (GCS buckets with sensitive data, Secret Manager).

### 4.12 Organization policies

- **What.** An **org policy** sets a **constraint** on a resource node (org, folder, project). It restricts **how resources can be configured**, for everybody, including Owners. It is the GCP equivalent of an AWS SCP or Azure Policy (deny effect).
- **How it works.** There are **list constraints** (allowed/denied values) and **boolean constraints** (enforce on/off). Policies are inherited; a child can **merge** with or **replace** the parent's policy only if the person has the org policy admin role (normally only at org level). You can also write **custom constraints** with CEL on resource fields (for supported services). A **dry-run** mode exists for some constraints; check docs.
- **Examples a data platform should know:**

| Constraint | Effect |
|---|---|
| `gcp.resourceLocations` = `in:eu-locations` | Resources only in EU locations (data residency). Use `in:europe-north1-locations` for Finland only. |
| `iam.allowedPolicyMemberDomains` | Only principals from your Workspace/Cloud Identity customer can be granted roles. Stops sharing with `gmail.com` and `allUsers`. |
| `iam.disableServiceAccountKeyCreation` | No SA keys |
| `iam.automaticIamGrantsForDefaultServiceAccounts` | Default SAs don't get Editor |
| `storage.publicAccessPrevention` | No public buckets |
| `storage.uniformBucketLevelAccess` | Buckets use IAM only, no object ACLs |
| `compute.vmExternalIpAccess` | No public IPs on VMs (Dataflow workers must be private) |
| `compute.skipDefaultNetworkCreation` | New projects don't get the wide-open `default` VPC |
| `sql.restrictPublicIp` | Cloud SQL without public IP |

- **Why it matters.** IAM says "who can". Org policies say "what is never OK". Even if a deployer SA is compromised, it can't create a US dataset or a public bucket.
- **Common mistake.** Rolling out `allowedPolicyMemberDomains` without checking existing bindings. Existing external grants keep working, but new ones fail, and some Google services that use special principals may need exceptions. Test in a nonprod folder first.

### 4.13 VPC Service Controls

- **What.** A **service perimeter** is a virtual fence around a set of **projects** and a set of **Google APIs** (e.g. BigQuery, GCS, Pub/Sub). API calls that would move data **across** the fence are blocked, even with valid IAM.
- **How it works.** Every call to a protected API is checked: is the caller (network origin, identity, device) inside or outside the perimeter, and is the resource inside or outside? Calls that cross are denied unless an **ingress rule**, **egress rule** or **access level** (from Access Context Manager, e.g. corporate IP ranges) allows them. You can run the perimeter in **dry-run** mode first and read the "would be denied" logs.
- **What exfiltration it prevents.**
    - An insider with read access runs `bq cp prj-ingest-prod:raw_pos.orders my-personal-project:x.orders`. IAM allows it (they can read and they own the other project). The **perimeter** blocks it, because the destination is outside.
    - A **stolen token** used from the public internet: blocked if the call doesn't come from an allowed network or device.
    - A query that writes results to a bucket outside the perimeter.
- **Why it matters.** IAM controls **who**. VPC-SC controls **where data can go**. For sales data with customer PII, this extra layer is important.
- **Common mistakes.** Turning it on in enforced mode without dry-run, and breaking: CI runners outside the perimeter, Looker or a SaaS tool calling BigQuery, cross-project jobs where one project is outside. Plan ingress/egress rules first.

### 4.14 Networking basics for data platforms

```text
                  ┌──────────────── prj-net-host-prod (Shared VPC host) ─────────────────┐
 On-prem DCs      │ VPC "vpc-prod" (global)                                              │
 (ERP, POS DB) ───┼──HA VPN / Interconnect──► Cloud Router                               │
                  │   subnet europe-north1  10.10.0.0/20  Private Google Access = ON     │
                  │   Cloud NAT (outbound internet without public IPs)                   │
                  │   PSC endpoint for Google APIs / Datastream private connectivity     │
                  └───────────▲───────────────────────────────▲──────────────────────────┘
                              │ uses subnets                  │ uses subnets
                  prj-ingest-prod (service project)    prj-orch-prod (service project)
                  Dataflow workers (no public IPs)     Composer (private IP)
```

| Piece | What it is | Why a data engineer cares |
|---|---|---|
| **VPC** | A global private network; **subnets** are regional | Dataflow workers and Composer get IPs from subnets |
| **Shared VPC** | The network lives in a **host project**; **service projects** use its subnets | The network team controls firewalls and on-prem routes centrally; data teams just deploy into it |
| **Private Google Access** | A subnet setting: VMs without external IPs can reach Google APIs | Private Dataflow workers can still call BigQuery and GCS |
| **Private Service Connect (PSC)** | A private IP **endpoint** in your VPC for Google APIs or a producer's service | Reach APIs or managed services privately; one of the private connectivity options for some services |
| **Cloud NAT** | Managed outbound NAT, per region | Private workers that must reach the internet (package downloads, external APIs) |
| **Cloud VPN / Interconnect** | Encrypted tunnels (HA VPN) or dedicated physical links to on-prem | Reaching on-prem source databases for Datastream or batch extracts |
| **Firewall rules** | Allow/deny traffic inside the VPC | Dataflow workers need to talk to each other on specific ports |

For Datastream specifically, the source DB can be reached by **IP allowlisting**, a **forward SSH tunnel**, or **private connectivity** (a private connection into your VPC). Check current docs for the options per source.

### 4.15 Secret Manager

- **What.** A managed store for secrets (DB passwords, API keys). A **secret** has **versions**; you add a new version to rotate.
- **How it works.** Access is IAM on the **secret**: `roles/secretmanager.secretAccessor` lets a principal read the value. Replication can be **automatic** or **user-managed** (pick EU regions for residency). Access is logged in audit logs. Composer can use Secret Manager as its **secrets backend** for Airflow connections. Some services (check Datastream's docs) can read connection passwords from Secret Manager directly.
- **Why it matters.** No passwords in DAG code, Terraform variables or state.
- **Common mistake.** Creating the secret **and its value** in Terraform. Then the password is in the Terraform **state file** in plain text. Better: Terraform creates the empty secret and the IAM; a human or a rotation job adds the version.

### 4.16 Monitoring, logging, labels and billing export

- **Cloud Monitoring** collects metrics (Pub/Sub backlog, Dataflow system lag, Datastream freshness, BigQuery slot usage) and runs **alert policies**. **Cloud Logging** holds logs; **log-based metrics** turn log lines into metrics (e.g. "count of failed load jobs").
- **Labels** are key-value pairs on resources (`team=sales`, `source=pos`, `env=prod`, `cost_center=cc123`). Lowercase, limited length. They appear in the **billing export**. BigQuery **jobs** can have labels too, so you can attribute query cost to a pipeline.
- **Tags** (Resource Manager tags) are different: they are used for **policy** (IAM conditions, org policy conditions, firewall), not for billing reports.
- **Billing export to BigQuery** sends detailed cost rows (per project, service, SKU, labels) to a dataset. Data starts flowing when you turn it on, so turn it on early. **Budgets** send alerts; they **don't stop** spending by themselves.
- **Why it matters.** A modernized platform should track metrics for load duration, throughput, stability and cost. Cost per source = billing export + labels + `INFORMATION_SCHEMA.JOBS` with job labels.

### 4.17 Terraform: how it works

#### The pieces

| Piece | What it is | Example |
|---|---|---|
| **Provider** | A plugin that knows one API | `hashicorp/google`, `hashicorp/google-beta` |
| **Resource** | Something Terraform creates and manages | `google_bigquery_dataset.raw` |
| **Data source** | Something Terraform only **reads** | `data "google_project" "this"` (to get the project number) |
| **Variable / output / local** | Inputs, results, computed helper values | `var.source_name`, `output "dataset_id"` |
| **Module** | A folder of `.tf` files called with inputs | `module "src_pos" { source = ... }` |
| **State** | Terraform's record of what it manages | `default.tfstate` in a GCS bucket |

#### The dependency graph

Terraform reads all `.tf` files and builds a **graph**. If resource B uses an attribute of A (`google_service_account.ingest.email`), B **depends on** A. Terraform creates independent resources **in parallel** and dependent ones in order. Use `depends_on` only for hidden dependencies (for example "the API must be enabled first").

#### Plan and apply

```text
 terraform init      download providers and modules, configure the backend
 terraform plan      1. refresh: read the real state of each resource from the APIs
                     2. compare: config (desired) vs. state (+ real values)
                     3. output: + create, ~ update in place, -/+ replace, - destroy
 terraform apply     execute the plan (ideally a saved plan file: plan -out=tf.plan)
```

The `-/+` (replace) lines are the dangerous ones. Some attribute changes **force replacement**: for example, changing a dataset's `location` or a bucket's `name` means destroy and recreate, which for a dataset means **losing data**.

#### Why the state file exists

Terraform needs to know which real object belongs to which resource address. The state maps `google_bigquery_dataset.raw` → `projects/prj-ingest-prod/datasets/raw_pos`, stores attribute values, and records dependencies. Without state, Terraform couldn't tell "I created this and must update it" from "someone else's resource with the same name". State can contain **secrets in plain text** (any sensitive attribute), so treat it as sensitive data.

#### Remote state on GCS and locking

```hcl
terraform {
  required_version = ">= 1.6"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }

  backend "gcs" {
    bucket = "example-tfstate-prod"
    prefix = "ingestion"
  }
}
```

- The GCS backend supports **locking** natively (it writes a lock object), so two applies can't run at the same time. No DynamoDB-like table is needed (unlike the classic S3 backend).
- Turn on **object versioning** on the state bucket so you can restore an old state after corruption.
- **One state bucket (or prefix) per environment**, and **small states** per component (network, ingestion, each domain). A huge single state is slow and has a big blast radius.

#### Workspaces vs. directory per environment

| | CLI workspaces | Directory per environment (`envs/dev`, `envs/prod`) |
|---|---|---|
| How | Same code and backend, `terraform workspace select prod` | Separate root modules, each with its own backend and tfvars |
| Pros | Less duplication | Clear separation: different backends, different credentials, different module versions per env |
| Cons | Same credentials and backend for all envs; easy to apply to the wrong one | Some duplication (kept small by modules) |
| Recommendation | Fine for short-lived feature stacks | **Prod data platforms**: dev can be on module `v1.5.0` while prod stays on `v1.4.0` |

### 4.18 Modules, loops, lifecycle, import, moved and drift

#### Modules
A module is a folder with `variables.tf` (inputs), `main.tf` (resources), `outputs.tf` (results), and `versions.tf` (required providers). You **version** modules with git tags (`?ref=v1.4.0`) or a private registry. Use **semantic versioning**: a breaking input change is a major version.

#### `count` vs. `for_each`

```hcl
# BAD for a list of things: count uses positions
resource "google_bigquery_dataset" "raw" {
  count      = length(var.sources)          # ["pos", "ecom", "erp"]
  dataset_id = "raw_${var.sources[count.index]}"
}
# Remove "pos" → "ecom" moves from index 1 to 0 → Terraform wants to
# REPLACE raw_ecom and raw_erp. That means deleting datasets.

# GOOD: for_each uses stable keys
resource "google_bigquery_dataset" "raw" {
  for_each   = toset(var.sources)
  dataset_id = "raw_${each.key}"
}
# Remove "pos" → only google_bigquery_dataset.raw["pos"] is destroyed.
```

Use `count` only for "0 or 1" (`count = var.enable_cdc ? 1 : 0`). Keys of `for_each` must be **known at plan time** (not computed from resources that don't exist yet).

#### `lifecycle`

```hcl
resource "google_bigquery_dataset" "raw" {
  dataset_id = "raw_pos"
  location   = "EU"

  lifecycle {
    prevent_destroy = true        # plan fails if anything would destroy it
    ignore_changes  = [labels["last_modified_by"]]  # don't fight an external tool
  }
}
```

- `prevent_destroy`: protect datasets and buckets with data. Note: it can't take a variable, and it doesn't help if someone removes the whole resource block (use a `removed` block or review for that).
- `ignore_changes`: when something outside Terraform legitimately changes an attribute.
- `create_before_destroy`: for replacements without downtime.
- The Google provider also has its own protections: `deletion_protection` on BigQuery tables, and `deletion_policy` on projects in recent provider versions (check docs).

#### Import and `moved` (brownfield work)

```hcl
# Bring an existing hand-made dataset under Terraform (Terraform 1.5+)
import {
  to = google_bigquery_dataset.raw["pos"]
  id = "projects/prj-ingest-prod/datasets/raw_pos"
}

# Refactor: the resource moved into a module. Without this, Terraform
# would destroy the old address and create a new one.
moved {
  from = google_bigquery_dataset.raw_pos
  to   = module.src_pos.google_bigquery_dataset.raw
}
```

The safe import loop: write the config → `plan` → adjust config until the plan shows **no changes** except the import → apply → enforce through CI. `terraform plan -generate-config-out=generated.tf` can draft the config for you.

#### Drift
**Drift** = the real world differs from state/config (someone clicked in the console). Detect it with a **scheduled `terraform plan`** (e.g. nightly) that alerts when the plan isn't empty, or `terraform plan -refresh-only`. Then either fix the config or let apply revert the manual change. Long-term fix: humans don't have write roles in prod; only the deployer SA has.

### 4.19 Google provider specifics

#### IAM resources: `_iam_member` vs. `_iam_binding` vs. `_iam_policy`

Most GCP resources have three Terraform IAM resource types:

| Resource | Authoritative for | Effect |
|---|---|---|
| `google_X_iam_member` | Nothing. **Additive**: one principal + one role | Adds one member; leaves everything else alone. **Default choice.** |
| `google_X_iam_binding` | **One role** on the resource | Sets the **complete member list** of that role. Removes anyone else who has that role. |
| `google_X_iam_policy` | **The whole policy** on the resource | Replaces **all** bindings. Anything not in your config is removed. |

**Danger example 1 (policy).** An engineer writes:

```hcl
data "google_iam_policy" "ingest" {
  binding {
    role    = "roles/owner"
    members = ["group:platform-admins@example.com"]
  }
}

resource "google_project_iam_policy" "ingest" {
  project     = "prj-ingest-prod"
  policy_data = data.google_iam_policy.ingest.policy_data
}
```

On apply, **every other binding on the project is deleted**: the pipeline SAs' roles, Composer's roles, and the **service agents** (`roles/dataflow.serviceAgent`, the Datastream and Pub/Sub agents). Dataflow jobs fail, Datastream stops, Pub/Sub push breaks. If the group binding is wrong, you can even **lock yourself out**. Recovery needs someone with rights from the folder or org.

**Danger example 2 (binding).** Team A's module sets:

```hcl
resource "google_bigquery_dataset_iam_binding" "readers" {
  project    = "prj-dwh-sales-prod"
  dataset_id = "mart_sales"
  role       = "roles/bigquery.dataViewer"
  members    = ["group:sales-analysts@example.com"]
}
```

Team B's config adds the Looker SA with an `_iam_member` for the same role. Every time team A applies, the **Looker SA is removed** and dashboards break. Then team B applies and adds it back. The two configs **fight** forever.

**Rule:** use `_iam_member` by default. Use `_iam_binding` only when this config is the **single owner** of that role on that resource. Avoid `_iam_policy` on projects, folders and the org.

**BigQuery-specific gotcha.** A dataset's access can also be managed with `access {}` blocks inside `google_bigquery_dataset` or with `google_bigquery_dataset_access`. The `access` blocks are **authoritative** for the dataset's access list. Don't mix them with `google_bigquery_dataset_iam_*` resources on the same dataset. The provider docs also warn that the authoritative dataset IAM resources can remove **authorized view** entries; for authorized views use `google_bigquery_dataset_access` (check current provider docs).

#### API enablement: `google_project_service`

```hcl
resource "google_project_service" "apis" {
  for_each = toset([
    "bigquery.googleapis.com",
    "datastream.googleapis.com",
    "dataflow.googleapis.com",
    "pubsub.googleapis.com",
    "secretmanager.googleapis.com",
  ])

  project            = "prj-ingest-prod"
  service            = each.key
  disable_on_destroy = false   # never switch off an API just because this resource is removed
}
```

- Resources in a project fail with "API not enabled" if this hasn't happened yet. Add `depends_on = [google_project_service.apis]` where Terraform can't see the dependency.
- `disable_on_destroy = false` matters: disabling an API on destroy can break running services that someone else depends on.
- Enabling an API can also create **service agents**; they may appear a little later (eventual consistency).

#### Other useful provider features
- `default_labels` in the provider block adds labels to every supported resource.
- `impersonate_service_account` in the provider block: run Terraform as a deployer SA without keys.
- `google` vs. `google-beta`: beta features need the beta provider.

### 4.20 CI/CD for infrastructure and Workload Identity Federation

#### The pipeline

```text
 feature branch ──► Pull Request
   ├─ terraform fmt -check            (style)
   ├─ terraform validate              (syntax, types)
   ├─ tflint                          (provider-aware lint: bad machine types, unused vars)
   ├─ checkov / trivy                 (security: public buckets, no CMEK, basic roles)
   ├─ conftest (OPA) on plan JSON     (our rules: EU only, labels required, no _iam_policy)
   ├─ terraform plan per env          → posted as a PR comment
   └─ review: CODEOWNERS (platform team for IAM/network/prod), 1–2 approvals
 merge to main
   ├─ apply dev   (automatic)
   ├─ apply test  (automatic, then smoke tests)
   └─ apply prod  (manual approval in a protected environment)
```

Policy as code reads the **plan**, not only the code, so it sees final values:

```bash
terraform plan -out=tf.plan
terraform show -json tf.plan > plan.json
conftest test plan.json --policy policies/
```

#### The deployer service account per environment

- `sa-tf-dev`, `sa-tf-test`, `sa-tf-prod`. Each has roles **only on its environment's folder** (and its state bucket). The dev deployer **can't** touch prod.
- Optionally split **plan** and **apply** identities: a **plan SA** with read-only roles (plus access to the state bucket for the lock) that any PR can use, and an **apply SA** that only the `main` branch in a protected environment can use.
- The deployer SA is the most powerful identity in the platform. Protect it: no keys, only CI can impersonate it, audit logs alerts on its use outside CI, and limit which roles it can grant (IAM conditions on role grants).

#### Workload Identity Federation (WIF), step by step

The problem: GitHub Actions runs outside GCP. The old way was a JSON key in a GitHub secret. WIF removes the key.

```text
 GitHub Actions job                Google STS                    IAM Credentials API
 ──────────────────                ──────────                    ───────────────────
 (1) Job asks GitHub for an
     OIDC ID token (JWT):
     iss = token.actions.githubusercontent.com
     sub = repo:example/infra:ref:refs/heads/main
     repository = example/infra, ref, environment, ...
         │
         │ (2) send JWT
         └──────────────────────► (3) STS checks: signature against
                                      GitHub's public keys; issuer and
                                      audience match the WIF provider;
                                      attribute mapping + attribute
                                      CONDITION (e.g. repository ==
                                      'example/infra') is true
                                  (4) returns a short-lived
                                      FEDERATED token for the principal
                                      principalSet://.../attribute.repository/example/infra
         ┌────────────────────────────────┘
         │ (5) use federated token to call generateAccessToken
         └──────────────────────────────────────────────────────► (6) IAM checks: does this
                                                                      principal have
                                                                      roles/iam.workloadIdentityUser
                                                                      ON sa-tf-prod?
                                                                  (7) returns an OAuth access
                                                                      token for sa-tf-prod (~1 h)
 (8) terraform uses the token to call GCP APIs. Nothing long-lived was ever stored.
```

Terraform for the WIF setup (in the `prj-iac` project):

```hcl
resource "google_iam_workload_identity_pool" "github" {
  project                   = "prj-iac"
  workload_identity_pool_id = "github"
  display_name              = "GitHub Actions"
}

resource "google_iam_workload_identity_pool_provider" "github" {
  project                            = "prj-iac"
  workload_identity_pool_id          = google_iam_workload_identity_pool.github.workload_identity_pool_id
  workload_identity_pool_provider_id = "github-oidc"

  attribute_mapping = {
    "google.subject"         = "assertion.sub"
    "attribute.repository"   = "assertion.repository"
    "attribute.ref"          = "assertion.ref"
    "attribute.environment"  = "assertion.environment"
  }

  # Only tokens from our organization's infra repo are accepted at all
  attribute_condition = "assertion.repository == \"example/infra\""

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

# Only jobs running in the protected GitHub environment "prod" may become sa-tf-prod
resource "google_service_account_iam_member" "prod_deployer_wif" {
  service_account_id = "projects/prj-iac/serviceAccounts/sa-tf-prod@prj-iac.iam.gserviceaccount.com"
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github.name}/attribute.environment/prod"
}
```

**Why the attribute condition matters.** GitHub signs tokens for **every** repository on GitHub. Without a condition on `repository` (or `repository_owner`), any repo in the world with the same issuer could try to use your pool. The condition is your main control.

The GitHub workflow:

```yaml
name: terraform-prod
on:
  pull_request:
    paths: ["envs/prod/**", "modules/**"]
  push:
    branches: [main]
    paths: ["envs/prod/**", "modules/**"]

permissions:
  id-token: write        # allow the job to request the OIDC token
  contents: read
  pull-requests: write   # post the plan as a comment

jobs:
  plan:
    if: github.event_name == 'pull_request'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: google-github-actions/auth@v2
        with:
          workload_identity_provider: projects/123456789/locations/global/workloadIdentityPools/github/providers/github-oidc
          service_account: sa-tf-plan-prod@prj-iac.iam.gserviceaccount.com
      - uses: hashicorp/setup-terraform@v3
      - run: terraform -chdir=envs/prod init -input=false
      - run: terraform -chdir=envs/prod plan -input=false -out=tf.plan
      # (post plan output as PR comment, run conftest on the plan JSON)

  apply:
    if: github.event_name == 'push'
    runs-on: ubuntu-latest
    environment: prod    # protected environment: required reviewers
    steps:
      - uses: actions/checkout@v4
      - uses: google-github-actions/auth@v2
        with:
          workload_identity_provider: projects/123456789/locations/global/workloadIdentityPools/github/providers/github-oidc
          service_account: sa-tf-prod@prj-iac.iam.gserviceaccount.com
      - uses: hashicorp/setup-terraform@v3
      - run: terraform -chdir=envs/prod init -input=false
      - run: terraform -chdir=envs/prod apply -input=false -auto-approve
```

(In a stricter setup the apply job applies the **exact plan file** that was reviewed, stored as a build artifact.)

---

## 5. What matters most in practice

### 5.1 The shared ingestion platform layout (start here)

```text
Folder prod
 ├── prj-ingest-prod                      (shared, owned by the platform team)
 │     ├── Datastream streams, Pub/Sub topics, Dataflow jobs
 │     ├── GCS: prj-ingest-prod-landing-<source>   (one bucket per source)
 │     └── BigQuery datasets (location EU)
 │           ├── raw_pos          ← one dataset PER SOURCE
 │           ├── raw_ecom
 │           ├── raw_erp_files
 │           └── ops_ingestion    ← load audit, freshness, reconciliation tables
 ├── prj-orch-prod                        (Composer; DAG factory reads source YAMLs)
 ├── prj-dwh-sales-prod                   (domain: stg_sales, mart_sales)
 ├── prj-dwh-finance-prod
 └── prj-bi-prod                          (Looker SA runs jobs here; BI reservation)
```

**The reasoning:**

- **Shared ingestion project**: one place for connectivity to sources (VPN, private connectivity), one set of standard patterns, one monitoring view, and no duplicated ingestion of the same source by two teams.
- **One dataset per source**: dataset is the BigQuery IAM unit, so "sales can read `raw_pos`" is one binding. Ownership and labels are clean. Deleting a source is deleting one dataset.
- **Domain projects for transformations**: domain teams own their logic and cost; they read raw data **cross-project**.
- **Separate orchestration and BI projects**: Composer has its own lifecycle and quotas; BI query cost is visible separately.
- **Same layout in dev and test**, created from the same modules.
- **Ingestion per environment, not one ingestion for all**: dev pipelines must not write to prod raw.

When might you split the ingestion project? If one source is very sensitive (HR, payments) and needs its own VPC-SC perimeter, or if quotas (e.g. Dataflow per-project limits) become a problem. It is a trade-off between isolation and simplicity.

### 5.2 The identity design

| Identity | Type | Purpose |
|---|---|---|
| `sa-ingest-<source>` | SA in `prj-ingest-*` | Runs that source's load: Dataflow worker, load jobs, reads its landing bucket |
| `sa-composer` | SA in `prj-orch-*` | The Composer environment; **impersonates** the per-source SAs for each task |
| `sa-dataform-<domain>` | SA in the domain project | Runs transformations for that domain |
| `sa-looker` | SA in `prj-bi-*` | Looker's BigQuery connection |
| `sa-tf-<env>`, `sa-tf-plan-<env>` | SA in `prj-iac` | CI deployers |
| `group:platform-admins` | Group | Admin on platform folder, break-glass |
| `group:<domain>-engineers` | Group | Read raw + write in dev of their domain |
| `group:<domain>-analysts` | Group | Read marts only |
| `group:ingest-oncall` | Group | Operate pipelines (Dataflow developer, view logs), time-bound escalation |

Why Composer impersonates instead of having all the rights itself: Composer is shared by many DAGs. If the Composer SA had write access to all raw datasets, any DAG (or any bug) could write anywhere. With impersonation, each task runs as its source's SA, and the logs show which source did what.

### 5.3 Least privilege, concretely

| Principal | Role | Resource |
|---|---|---|
| `sa-ingest-pos` | `roles/bigquery.dataEditor` | dataset `prj-ingest-prod.raw_pos` **only** |
| `sa-ingest-pos` | `roles/bigquery.jobUser` | project `prj-ingest-prod` (load jobs run here) |
| `sa-ingest-pos` | `roles/storage.objectAdmin` | bucket `prj-ingest-prod-landing-pos` only |
| `sa-ingest-pos` | `roles/dataflow.worker` | project `prj-ingest-prod` (if it is a Dataflow worker SA) |
| `sa-ingest-pos` | `roles/secretmanager.secretAccessor` | secret `src-pos-credentials` only |
| `sa-composer` | `roles/iam.serviceAccountTokenCreator` | **on** `sa-ingest-pos` (the SA as a resource) |
| `sa-dataform-sales` | `roles/bigquery.dataViewer` | datasets `raw_pos`, `raw_ecom` |
| `sa-dataform-sales` | `roles/bigquery.dataEditor` | datasets `stg_sales`, `mart_sales` |
| `sa-dataform-sales` | `roles/bigquery.jobUser` | project `prj-dwh-sales-prod` |
| `group:sales-analysts` | `roles/bigquery.dataViewer` | dataset `mart_sales` |
| `group:sales-analysts` | `roles/bigquery.jobUser` | project `prj-bi-prod` |
| `sa-looker` | `roles/bigquery.dataViewer` + `jobUser` | marts + `prj-bi-prod` |

Note what is **missing**: no project-level `dataViewer`, no basic roles, no `bigquery.admin`, no `serviceAccountUser` on the project.

### 5.4 Walkthrough: the "onboard a new source" module

The goal: **onboarding a source = one module block in a PR**. Everything a source needs, created the same way every time.

#### `modules/ingestion_source/versions.tf`

```hcl
terraform {
  required_version = ">= 1.6"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 6.0, < 7.0"
    }
  }
}
```

A module declares which provider versions it supports, but **does not** configure the provider or the backend. The root module (`envs/prod`) does that. This keeps the module reusable for every environment.

#### `modules/ingestion_source/variables.tf`

```hcl
variable "project_id" {
  description = "The shared ingestion project, e.g. prj-ingest-prod."
  type        = string
}

variable "env" {
  description = "Environment name: dev, test or prod."
  type        = string

  validation {
    condition     = contains(["dev", "test", "prod"], var.env)
    error_message = "env must be dev, test or prod."
  }
}

variable "source_name" {
  description = "Short source name, lowercase, e.g. pos."
  type        = string

  validation {
    condition     = can(regex("^[a-z][a-z0-9_]{1,20}$", var.source_name))
    error_message = "source_name must be lowercase letters, digits or _, max 21 chars."
  }
}

variable "source_type" {
  description = "postgres, mysql, gcs_files or pubsub_events."
  type        = string
}

variable "owner_domain" {
  description = "Owning domain team, e.g. sales. Used for labels and cost attribution."
  type        = string
}

variable "location" {
  description = "BigQuery and GCS location. Must match the rest of the warehouse."
  type        = string
  default     = "EU"
}

variable "reader_groups" {
  description = "Groups that may read the raw dataset."
  type        = list(string)
  default     = []
}

variable "reader_service_accounts" {
  description = "Service accounts (e.g. Dataform SAs) that may read the raw dataset."
  type        = list(string)
  default     = []
}

variable "orchestrator_sa" {
  description = "Composer SA email that will impersonate this source's SA."
  type        = string
}

variable "composer_bucket" {
  description = "Composer environment bucket, where the DAG factory reads configs."
  type        = string
}

variable "freshness_sla_minutes" {
  description = "Maximum allowed data age before alerting."
  type        = number
  default     = 60
}

variable "contains_pii" {
  description = "True if the source has personal data. Drives labels and policy checks."
  type        = bool
  default     = false
}

variable "needs_credentials" {
  description = "True if the source needs a DB password or API key."
  type        = bool
  default     = false
}

variable "secret_region" {
  description = "Region for user-managed secret replication."
  type        = string
  default     = "europe-north1"
}

variable "landing_retention_days" {
  description = "Days before landing files move to a colder storage class."
  type        = number
  default     = 30
}
```

Line by line, the important points:

- **`validation` blocks** catch mistakes at `plan` time, before anything reaches GCP. A wrong `env` or an uppercase source name fails in CI, not in production.
- **`location` has a default** of the warehouse's location, so teams can't easily create a dataset in the wrong region. (The org policy is the second line of defence.)
- **`reader_groups` and `reader_service_accounts`** are separate lists so the module can build the correct principal prefix (`group:` vs `serviceAccount:`).
- **`contains_pii`** is not just documentation: it becomes a label, and policy-as-code can require extra controls (e.g. policy tags, no wide readers) when it is `true`.
- **`orchestrator_sa`** is an input, not hard-coded, so the same module works with the dev and prod Composer.

#### `modules/ingestion_source/main.tf`

```hcl
locals {
  labels = {
    source       = var.source_name
    source_type  = var.source_type
    owner_domain = var.owner_domain
    env          = var.env
    pii          = var.contains_pii ? "true" : "false"
    managed_by   = "terraform"
  }

  reader_members = concat(
    [for g in var.reader_groups : "group:${g}"],
    [for s in var.reader_service_accounts : "serviceAccount:${s}"],
  )
}

# 1. One identity per source
resource "google_service_account" "ingest" {
  project      = var.project_id
  account_id   = "sa-ingest-${replace(var.source_name, "_", "-")}"
  display_name = "Ingestion SA for source ${var.source_name}"
  description  = "Owned by ${var.owner_domain}. Managed by Terraform."
}

# 2. Landing bucket for files (or Dataflow temp/staging)
resource "google_storage_bucket" "landing" {
  project                     = var.project_id
  name                        = "${var.project_id}-landing-${replace(var.source_name, "_", "-")}"
  location                    = var.location
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false
  labels                      = local.labels

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = var.landing_retention_days
    }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }
}

# 3. Raw dataset: one per source
resource "google_bigquery_dataset" "raw" {
  project     = var.project_id
  dataset_id  = "raw_${var.source_name}"
  location    = var.location
  description = "Raw data from ${var.source_name}. Owner: ${var.owner_domain}."
  labels      = local.labels

  lifecycle {
    prevent_destroy = true
  }
}

# 4. The source SA can write only to its own dataset
resource "google_bigquery_dataset_iam_member" "writer" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.raw.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.ingest.email}"
}

# 5. ...and run load/query jobs in the ingestion project
resource "google_project_iam_member" "job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.ingest.email}"
}

# 6. ...and manage objects in its own landing bucket only
resource "google_storage_bucket_iam_member" "landing_rw" {
  bucket = google_storage_bucket.landing.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.ingest.email}"
}

# 7. Readers (domain groups and transformation SAs)
resource "google_bigquery_dataset_iam_member" "readers" {
  for_each   = toset(local.reader_members)
  project    = var.project_id
  dataset_id = google_bigquery_dataset.raw.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = each.value
}

# 8. Composer may impersonate this SA (per-task identity)
resource "google_service_account_iam_member" "orchestrator_impersonation" {
  service_account_id = google_service_account.ingest.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:${var.orchestrator_sa}"
}

# 9. A secret container for credentials (the VALUE is added outside Terraform)
resource "google_secret_manager_secret" "credentials" {
  count     = var.needs_credentials ? 1 : 0
  project   = var.project_id
  secret_id = "src-${replace(var.source_name, "_", "-")}-credentials"
  labels    = local.labels

  replication {
    user_managed {
      replicas {
        location = var.secret_region
      }
    }
  }
}

resource "google_secret_manager_secret_iam_member" "credentials_access" {
  count     = var.needs_credentials ? 1 : 0
  project   = var.project_id
  secret_id = google_secret_manager_secret.credentials[0].secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.ingest.email}"
}

# 10. Config for the Composer DAG factory, generated from the same inputs
resource "google_storage_bucket_object" "dag_config" {
  bucket = var.composer_bucket
  name   = "dags/configs/sources/${var.source_name}.yaml"

  content = yamlencode({
    source                = var.source_name
    source_type           = var.source_type
    owner_domain          = var.owner_domain
    target_project        = var.project_id
    target_dataset        = google_bigquery_dataset.raw.dataset_id
    landing_bucket        = google_storage_bucket.landing.name
    service_account       = google_service_account.ingest.email
    freshness_sla_minutes = var.freshness_sla_minutes
  })
}

# (Optional, by source_type: Datastream connection profile + stream for
#  postgres/mysql, a Pub/Sub topic + BigQuery subscription for events,
#  and a Cloud Monitoring alert policy on freshness. Left out to keep the
#  example readable; they follow the same count/for_each pattern.)
```

Line by line:

- **`locals.labels`**: one label map used everywhere. Billing export and `INFORMATION_SCHEMA` can then group cost by `source` and `owner_domain`. `managed_by = terraform` tells people not to click-edit it.
- **`reader_members`**: builds full principal strings from simple inputs, so callers can't make typos like forgetting `group:`.
- **(1) `google_service_account`**: `account_id` must be 6–30 characters, lowercase letters, digits and hyphens, so we replace `_` with `-`. (A long source name could exceed the limit; a validation could check that too.)
- **(2) bucket**: bucket names are **global** across all of GCS, so we prefix with the project ID. `uniform_bucket_level_access` = IAM only. `public_access_prevention = "enforced"` = can never be public. `force_destroy = false` means Terraform won't delete a bucket that still has files. `versioning` protects against accidental overwrite. The `lifecycle_rule` moves old files to Coldline to save money.
- **(3) dataset**: `location` comes from the variable (default EU). `prevent_destroy` stops any plan that would delete raw data, for example if someone changes the location (which forces replacement).
- **(4) writer**: `dataEditor` on **this dataset only**, with `_iam_member` (additive, safe).
- **(5) job user**: load jobs are jobs, so the SA needs `jobUser` on the project where they run. This is project-level because jobs are project-level; it gives no data access by itself.
- **(6) bucket IAM**: bucket-level, not project-level, so `sa-ingest-pos` can't read the ecom landing files.
- **(7) readers**: `for_each` over a **set**, so adding or removing one reader changes only that binding.
- **(8) impersonation**: the binding is **on the SA resource** (`service_account_id`), not on the project. Composer can impersonate this SA and no others.
- **(9) secret**: `count` makes it optional. Replication is **user-managed** in one EU region (here `europe-north1`) for residency. We create the container and the access, **not the value**, so no password ends up in state.
- **(10) DAG config**: Terraform writes a YAML file that the Composer **DAG factory** reads. So one PR creates the infrastructure **and** the pipeline. There is no second manual step to forget.

#### `modules/ingestion_source/outputs.tf`

```hcl
output "dataset_id" {
  description = "Raw dataset ID."
  value       = google_bigquery_dataset.raw.dataset_id
}

output "service_account_email" {
  description = "The source's ingestion SA."
  value       = google_service_account.ingest.email
}

output "landing_bucket" {
  description = "Landing bucket name."
  value       = google_storage_bucket.landing.name
}

output "secret_id" {
  description = "Secret ID for credentials, or null."
  value       = var.needs_credentials ? google_secret_manager_secret.credentials[0].secret_id : null
}
```

Outputs let other configs **use** the results (e.g. the sales domain config can reference the raw dataset ID) without hard-coding names.

#### Usage: one block per source, or a YAML list

```hcl
# envs/prod/sources.tf
module "src_pos" {
  source = "git::https://github.com/example/tf-modules.git//ingestion_source?ref=v1.4.0"

  project_id              = "prj-ingest-prod"
  env                     = "prod"
  source_name             = "pos"
  source_type             = "postgres"
  owner_domain            = "sales"
  reader_groups           = ["sales-engineers@example.com"]
  reader_service_accounts = ["sa-dataform-sales@prj-dwh-sales-prod.iam.gserviceaccount.com"]
  orchestrator_sa         = "sa-composer@prj-orch-prod.iam.gserviceaccount.com"
  composer_bucket         = "europe-north1-composer-prod-bucket"
  freshness_sla_minutes   = 15
  contains_pii            = true
  needs_credentials       = true
}
```

At scale, teams don't even write HCL. They add a YAML file, and the root module loops over all of them:

```hcl
locals {
  source_files = fileset("${path.module}/sources", "*.yaml")
  sources = {
    for f in local.source_files :
    trimsuffix(f, ".yaml") => yamldecode(file("${path.module}/sources/${f}"))
  }
}

module "sources" {
  source   = "git::https://github.com/example/tf-modules.git//ingestion_source?ref=v1.4.0"
  for_each = local.sources

  project_id            = "prj-ingest-prod"
  env                   = "prod"
  source_name           = each.key
  source_type           = each.value.source_type
  owner_domain          = each.value.owner_domain
  reader_groups         = try(each.value.reader_groups, [])
  orchestrator_sa       = "sa-composer@prj-orch-prod.iam.gserviceaccount.com"
  composer_bucket       = "europe-north1-composer-prod-bucket"
  freshness_sla_minutes = try(each.value.freshness_sla_minutes, 60)
  contains_pii          = try(each.value.contains_pii, false)
  needs_credentials     = try(each.value.needs_credentials, false)
}
```

### 5.5 Keyless CI/CD with Workload Identity Federation
See 4.20. In short: *"GitHub issues an OIDC token, Google STS validates it against the WIF provider with an attribute condition on our repository, returns a federated token, and that identity impersonates the per-environment deployer SA through `workloadIdentityUser`. Tokens live about an hour. No key exists anywhere."*

### 5.6 Protecting the state and the deployer
- State bucket in a dedicated `prj-iac` project, **versioning on**, uniform access, public access prevention, optional CMEK.
- Only the deployer SAs (and a break-glass group) have `storage.objectAdmin` on it. Engineers don't need to read prod state.
- No secrets as Terraform inputs or outputs (state stores them in plain text). Mark sensitive variables `sensitive = true` (this hides them in output, but **not** in state).
- The deployer SA: roles only on its env folder; WIF only from the protected branch/environment; alert on its use from anywhere except CI; limit the roles it can grant.

### 5.7 Brownfield: bringing a hand-made platform under Terraform
1. Inventory existing resources (Cloud Asset Inventory can export them).
2. Write module calls for them and add `import` blocks.
3. `plan` until it shows only imports and **no changes**. Differences mean either the config is wrong or the real resource has drift that you decide on.
4. Apply. Remove humans' write access in prod. From now on, every change goes through PRs.
5. Do IAM carefully: import existing bindings as `_iam_member`, never replace a whole policy.

### 5.8 Cost attribution
- Projects per domain → cost per domain without any work.
- Labels on every resource (`source`, `owner_domain`, `env`) via the module → cost per source in the billing export.
- **Job labels** on BigQuery jobs (set by the DAG factory: `dag_id`, `task_id`, `source`) → query cost per pipeline from `INFORMATION_SCHEMA.JOBS` (`total_bytes_billed`, `total_slot_ms`).
- Budgets with alerts per project.
- A small dashboard: cost per source per day, next to load duration and freshness (the core platform metrics).

### 5.9 Adoption and self-service (the "without you" part)
- **Golden path**: the easy way is the correct way. The module gives naming, labels, IAM, alerts and DAG config automatically.
- **Template repo** with a working example source. Copy, rename, open a PR.
- **Docs**: README per module (inputs, outputs, examples, "how to onboard a source in 30 minutes"), generated with `terraform-docs`.
- **Guardrails in CI**: mistakes are caught by tools, not by tired reviewers.
- **Versioned modules + changelog**: teams upgrade on purpose. Deprecate old versions with a date.
- **Handover**: pair with the internal team on the first sources, then they do the next ones while the platform team reviews, then they own it.

---

## 6. Mapping from AWS and Azure

| Concept | AWS | Azure / Fabric | GCP |
|---|---|---|---|
| Root of everything | AWS Organization | Entra tenant + root management group | **Organization** |
| Grouping | Organizational Unit (OU) | Management group | **Folder** |
| Isolation + billing unit | **Account** | Subscription (+ resource group) | **Project** |
| Workspace for data | (S3 bucket / Glue database) | Fabric **workspace** | **BigQuery dataset** (IAM unit) |
| Human identity | IAM Identity Center user/group | Entra ID user/group | Google account / **Google Group** (often synced from Entra) |
| Workload identity | IAM **role** assumed by a service (instance profile) | **Managed identity** | **Service account** (attached or impersonated) |
| Assume a role | `sts:AssumeRole` | Use a managed identity / role assignment | **Impersonation** (`serviceAccountTokenCreator`) |
| "Pass" a role to a service | `iam:PassRole` | (assigning a managed identity to a resource) | **`iam.serviceAccounts.actAs`** (`serviceAccountUser`) |
| Permissions document | IAM policy JSON (Allow/Deny statements, attached to a principal) | Role definition + role assignment at a scope | **Role** + **binding** in the **resource's** allow policy |
| Hard guardrails | **SCP** | Azure Policy (deny) | **Organization policy** (+ deny policies) |
| Data exfiltration fence | VPC endpoint policies, S3 bucket policies with conditions | Private endpoints, network rules | **VPC Service Controls** |
| Private access to services | VPC endpoints / PrivateLink | Private endpoints | **Private Google Access / Private Service Connect** |
| Central network | Transit Gateway, shared VPC via RAM | Hub-spoke VNets | **Shared VPC** |
| Secrets | Secrets Manager | Key Vault | **Secret Manager** |
| Audit | CloudTrail | Activity log | **Cloud Audit Logs** |
| Cost tags | Cost allocation tags | Tags | **Labels** (+ billing export to BigQuery) |
| CI without keys | GitHub OIDC → IAM role | Federated credentials on an app/managed identity | **Workload Identity Federation** |
| Terraform remote state | S3 backend + DynamoDB lock | azurerm backend (blob lease) | **GCS backend** (lock built in) |

**The big mental shift from AWS.** In AWS, you usually attach a policy **to the principal** ("this role can read bucket X"). In GCP, the policy lives **on the resource** ("dataset X lets these principals read it"). To answer "what can this SA do?", you search across resources (Policy Analyzer). To answer "who can read this dataset?", you look at the dataset and its ancestors.

---

## 7. Hands-on exercise

### Part A: a design exercise (paper, 45 minutes)

**Scenario.** ShopFlow, the course's e-commerce company, has 3 sources: a POS Postgres database (on-prem), an e-commerce MySQL (Cloud SQL), and daily ERP CSV files from SFTP. Domains: sales and finance. Finance must not see e-commerce customer emails. Everything must stay in the EU.

Write down:

1. The folder and project tree for dev and prod.
2. The datasets in the ingestion project.
3. A table of every SA and group with its roles and resources.
4. Which org policies you set and where.
5. How finance reads e-commerce orders without emails. (Answer: an authorized view in a finance-facing dataset that excludes the column, or policy tags with masking on the email column.)
6. How a new source is onboarded in one PR.

Compare your answer with section 5.

### Part B: real Terraform in a free/trial project (60–90 minutes)

You need one GCP project (free trial). BigQuery sandbox alone is not enough for IAM.

```bash
# 1. Authenticate and set the project
gcloud auth application-default login
gcloud config set project YOUR_PROJECT_ID

# 2. Create a state bucket (versioned)
gcloud storage buckets create gs://YOUR_PROJECT_ID-tfstate --location=EU --uniform-bucket-level-access
gcloud storage buckets update gs://YOUR_PROJECT_ID-tfstate --versioning
```

Then:

1. Copy the module from 5.4 into `modules/ingestion_source/`. Create `envs/dev/main.tf` with the backend, the provider (`project`, `region = "europe-north1"`), and one module call. Use your own email as a reader with `reader_groups = []` and add a separate `google_bigquery_dataset_iam_member` for `user:you@...` (only for this lab).
2. Run `terraform init`, `plan`, `apply`. Read the plan: count the resources and check the dependency order.
3. **See the `count` vs `for_each` difference**: add two readers, remove the first one, and look at the plan.
4. **See `prevent_destroy`**: change `location` to `US` and run `plan`. It should fail.
5. **See drift**: add a label to the dataset in the console, run `plan`. Terraform wants to remove it.
6. **See impersonation**: create a second SA, give yourself `serviceAccountTokenCreator` on it, and run `gcloud storage ls --impersonate-service-account=...`. Then look at the audit log entry: both identities are there.
7. **Import**: create a dataset by hand with `bq mk`, then write an `import` block and plan until no changes.
8. `terraform destroy` at the end (remove `prevent_destroy` first). Notice that it refuses to delete a non-empty bucket because of `force_destroy = false`.

---

## 8. Glossary

| Term | Meaning |
|---|---|
| **Access level** | A condition (IP range, device, identity) from Access Context Manager used by VPC-SC |
| **actAs** | The permission `iam.serviceAccounts.actAs`: attach a service account to a resource |
| **Allow policy** | The list of bindings on one resource |
| **Attribute condition (WIF)** | A CEL rule on the external token's claims that must be true to accept the token |
| **Audit logs** | Admin Activity, Data Access, System Event and Policy Denied logs |
| **Authorized view** | A view allowed to read a dataset that its users can't read directly |
| **Basic role** | Owner, Editor, Viewer. Too broad for prod |
| **Billing account** | Pays for linked projects; outside the resource hierarchy |
| **Billing export** | Detailed cost data exported to a BigQuery dataset |
| **Binding** | Role + principals (+ optional condition) on a resource |
| **CEL** | Common Expression Language, used for IAM conditions and custom constraints |
| **CODEOWNERS** | A git file that makes reviews from certain teams mandatory for certain paths |
| **Custom role** | A role you define with a chosen list of permissions (org or project level) |
| **Deny policy** | A policy that blocks permissions for principals, evaluated before allow |
| **Drift** | Differences between real resources and Terraform state/config |
| **Effective policy** | Union of a resource's allow policy and all its ancestors' |
| **etag** | A version tag on a policy used to avoid overwriting concurrent changes |
| **Folder** | A grouping node between the organization and projects |
| **Golden path** | The supported, easy, standard way to do a task |
| **Impersonation** | Getting short-lived tokens for an SA via `serviceAccountTokenCreator` |
| **Labels** | Key-value metadata on resources, used for cost reporting |
| **Location (BigQuery)** | Region or multi-region where a dataset lives; fixed at creation |
| **Module (Terraform)** | A reusable folder of Terraform code with inputs and outputs |
| **Org policy** | A constraint on resource configuration, inherited down the hierarchy |
| **Permission** | One allowed action, e.g. `bigquery.jobs.create` |
| **Plan / apply** | Terraform's "show the changes" and "make the changes" steps |
| **Predefined role** | A role maintained by Google |
| **Principal** | An identity that can be granted a role (user, group, SA, federated identity) |
| **Private Google Access** | Lets VMs without public IPs reach Google APIs |
| **Private Service Connect** | Private endpoints in your VPC for Google APIs or managed services |
| **Project** | Container for resources; boundary for APIs, billing, quotas, IAM |
| **Resource hierarchy** | Organization → folders → projects → resources |
| **Secret Manager** | Managed storage for secrets with versions and IAM |
| **Service account** | An identity for workloads; also a resource that others can be granted roles on |
| **Service agent** | A Google-managed SA that a Google service uses in your project |
| **Service perimeter** | The VPC-SC fence around projects and APIs |
| **Shared VPC** | A network in a host project used by service projects |
| **State (Terraform)** | The record mapping code addresses to real resources |
| **STS** | Security Token Service; exchanges external tokens for Google federated tokens |
| **Tags (Resource Manager)** | Key-value tags used in policies (not for billing) |
| **Workload Identity Federation** | Lets external workloads use GCP without SA keys, via token exchange |
| **Workspace (Terraform)** | Multiple named states for the same configuration |
