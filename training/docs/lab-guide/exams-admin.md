# 🗂 Exams (managers)

Only lab managers see this page (**Manage → Exams** in the sidebar). Here you make the mock exams that
members take on **Extras → Mock exams** ([📝 Mock exams](mock-exams.md)): add their question banks,
choose who may take them, and see the results. An exam is a pool of questions from one or more
**question banks**; each attempt draws its questions from that pool.

## All exams
The list shows each exam with **Status** (**● Enabled** / **○ Disabled**, switched in one click),
**Who can take it**, **Questions** (the pool), **Per attempt**, **Pass mark**, **Attempts**, **Average**
and **In progress**. **Search exams…** and the buttons **All**, **DE lab**, **Other**, **Enabled**,
**Disabled** filter it. Click an exam to manage it.

- **+ New exam**: type a name, then **Create**. A new exam is disabled until you enable it.
- **⟳ Sync DE exams**: loads the course chapters' exams (**DE lab**) from their public GitHub folder:
  new chapters become exams open to all members, and new and updated questions come in. The result says
  *Sync completed*, plus what to fix when a file on GitHub has a mistake.

The lab also starts with a small demo exam (10 questions, open to everyone). You may change, close or
delete it; it doesn't come back.

## Add a question bank
On the exam's **Question banks** tab, **+ Add question bank**, pick one of three ways, then **Upload**:

| Way | What you give |
|---|---|
| **Encrypted file** | a question bank you received encrypted, and its **Key** (paste it or choose the `.key` file). The key is used once and not kept |
| **Link** | a public file on GitHub, or an AWS S3 presigned URL (it works until it expires). No key; **Add from link** downloads it once |
| **Own file** | a plain JSON bank you made. **⬇ Download the template** gives one question of each kind (one answer, several answers, hotspot, drag and drop, yes / no) and a case study |

The result says what changed: *new · updated · removed · unchanged · left out*. Adding a bank the exam
already has **updates** it: changed questions get a new version, questions missing from the file leave
the pool. Submitted attempts keep the questions they were marked on. A plain `exam.json` with the
exam's details (provider, domains) works as an **Own file** too.

Questions the file marks *needs-fix*, or that have a mistake, are **left out**; **why** in the
**Upload history** lists them. A DE lab exam's banks come only from **⟳ Sync DE exams**.

**Remove** on a bank takes its questions out of the pool (old reviews keep them).

## The exam's tabs

| Tab | What it shows and does |
|---|---|
| **Question banks** | the banks (**Questions**, **Updated**, **Left out**, **Last upload**) and the **Upload history** |
| **Questions** | every question with its right answer and explanation; filter by bank, type, domain, tag or *Need fix*, or search the text |
| **Settings** | see below |
| **In progress** | attempts started or paused, not submitted (**Member**, **Started**, **Answered**). **Delete** one, or **Delete all in progress**: that member's next start draws a new set with the current settings |
| **Results** | **Per member** (attempts, passed, best, average, last attempt) and **Latest attempts** (when, member, score, result, time) |

## Settings

| Field | What it means |
|---|---|
| **Name** | the name members see |
| **Who can take it** | **All members**, or **Chosen teams** (tick at least one) |
| **Questions per attempt** | 1 up to the pool size |
| **Pass mark (%)** | 1–100 |
| **Enabled — members can start it** | off: no new attempts; one in progress can only be submitted as it is. Past results stay visible |
| **Members see with each question** | **Domain** and **Tags** (the results per domain stay either way) |

**Save** applies to new attempts. Attempts in progress keep their questions; delete them under **In
progress** to start them with the new settings.

**Delete exam** (in the **Delete** card, or **Delete** in the list) asks first, inline. Members no longer
see the exam, nor their attempts and results for it. Deleting a member (**Manage → Members**) removes
their attempts too.

## What members see
An enabled exam open to them, once it has questions. A disabled exam shows only to members who have
attempts in it, marked *closed*. Correct answers never reach the browser before an attempt is submitted.
