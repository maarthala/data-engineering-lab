# 🪣 Storage

**Tools → Storage** opens the RustFS console: the lab's object storage (S3), where your files and the
files of your tables live. It shows the same bucket as [📁 My files](my-files.md), with the tools of an
object store: buckets, folders (prefixes), uploads and downloads.

## Signing in
Opening **Storage** from the portal signs you in with your lab account (single sign-on, OIDC). There is
no separate storage password. What you may see and do comes with your account.

## What you see
| Bucket | Whose | You may |
|---|---|---|
| `<you>-lake` (e.g. `demouser-lake`) | yours | list, read, upload, delete |
| `team-<team>-lake` (e.g. `team-sales-lake`) | your team's, when you're in a team | the same; the whole team writes it |
| a folder of another member's bucket | shared with you | read, or read and write, as the share says |

Other members' buckets don't show. Lab managers see every bucket. A bucket's settings are read only for
members, and its size limit is set by lab managers on [⚙️ Settings](settings.md).

Your bucket's layout:

| Folder | What's in it |
|---|---|
| `files/src/notebooks/`, `files/src/dags/`, `files/src/dbt/` | your code, kept in sync with Jupyter's `my_files/` ([📓 Notebooks](notebooks.md)) |
| `files/source/` | raw files to ingest |
| `tables/` | the files of your catalogs' tables: `tables/<catalog>/<namespace>/<table>/` |

`tables/` belongs to the catalog: create and drop tables with SQL or Spark, don't change files there.
`files/` is yours to use freely.

## Storage or My files?
Both show the same objects. **My files** is the portal's simpler view of your own bucket; the Storage
console is the object store's own UI. A file you upload in either one is in your bucket at once: a DAG
uploaded to `files/src/dags/` reaches [🔀 Pipelines](pipelines.md) about 30 seconds later, a notebook in
`files/src/notebooks/` reaches Jupyter at your server's next start.

## Related lessons
- [Your lab account & workspace](../setup/workspace.md)
- [Upload a file & register it as a table](../unit3/upload-register.md)
- [Read the lake (Bronze)](../unit4/read-bronze.md)
