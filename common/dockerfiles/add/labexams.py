"""labexams — mock certification exams on the portal (home-api), kept in the portal database.

Managers create an exam (a name), then add question banks to it (upload()): a file they receive
encrypted (certification-exams format, AES-256-GCM — see decrypt()) together with the key — used
once, never stored; a link to the plain file — a public GitHub file or an S3 presigned URL, fetched
here (fetch_link()); or a plain file they made themselves from the template (exam-template.json). Any number of banks make up one exam's question pool — from one
certification (dump1, dump2 …) or several. A bank is known in its exam by "<file exam_id>/<source>"
(e.g. dp-700/dump1) and a question by that plus its id (dp-700/dump1/dump1-042).

Uploading a bank again updates it: the upload is checked first (preview — what is new, changed,
removed, unchanged) and applied on confirm. A changed question gets a new version (own uuid);
the old version is kept, so submitted attempts keep showing exactly what they were marked on.
Questions missing from the new file are retired (out of the pool, still in old reviews). An
attempt holds its question versions: it isn't changed by later uploads. An optional plain
exam.json gives the provider and domains (and creates the exam when there is none yet).

Managers set how many questions an attempt has, the pass mark, and who may take it: everyone,
chosen teams, or nobody (off). They see the attempts in progress and can delete them (one or all
of an exam) — the member's next start then draws a new set from the current questions.

Members take an exam any number of times. Each attempt draws its questions at random, preferring
the ones the member has seen least; answers are saved as they go, so leaving is a pause and the
attempt resumes where it was. No time limit. After submitting: score, pass/fail, per domain and
a full review. Correct answers never reach the browser before the attempt is submitted.

Question banks are schema 1.0 or 1.1. 1.1 adds slot questions — hotspot (a dropdown per box),
drag-drop (values from the choice_pool onto targets or steps; shown as a dropdown per slot) and
yes-no (one per statement) — graded per slot: each slot is a share of the question's point. It also
adds case studies, stored once per bank (versioned like questions) and shown with each question
that refers to one. Text is Markdown with inline HTML; the page renders it and keeps only harmless
tags.

Questions marked review.status = needs-fix are left out; review.answer / review.slot_answers /
review.explanation win over the source's when present.

A demo exam (exam_demo.json: 10 questions, open to everyone) is added once, when the tables are
made; managers may change, close or delete it — it doesn't come back.

DB: PORTAL_DB_URL (postgres://portal:…@postgres:5432/portal); tables are made on first use, and the
first layout (one row per question, no versions) is moved over then (_migrate_v1).
"""
import base64
import gzip
import hashlib
import json
import os
import random
import re
import ipaddress
import socket
import threading
import urllib.error
import urllib.parse
import urllib.request
import uuid

DB_URL = os.environ.get("PORTAL_DB_URL", "postgresql://portal:portal@postgres:5432/portal")
FORMAT = "certification-exams/question-bank+aes-256-gcm"
EXAM_ID = re.compile(r"^[a-z0-9]+(-[a-z0-9]+)*$")
AUDIENCES = ("all", "teams", "off")
SLOT_TYPES = ("hotspot", "drag-drop", "yes-no")
MAX_BYTES = 32 << 20
GITHUB_HOSTS = ("raw.githubusercontent.com", "gist.githubusercontent.com")
S3_HOST = re.compile(r"^([a-z0-9.-]+\.)?s3([.-][a-z0-9-]+)*\.amazonaws\.com$")
DEMO = os.path.join(os.path.dirname(os.path.abspath(__file__)), "exam_demo.json")
_ready = False
_lock = threading.Lock()

# exam_banks: the banks of an exam (bank = "<file exam_id>/<source>"); exam_qv: question versions —
# current = in the pool (one per exam + bank + qid); exam_cv: case study versions; an attempt's
# vids are the question versions it was drawn with, its answers are keyed by vid.
SCHEMA = """
CREATE TABLE IF NOT EXISTS exams (
  id text PRIMARY KEY, name text NOT NULL, provider text, domains jsonb NOT NULL DEFAULT '[]',
  num_questions int NOT NULL DEFAULT 60, pass_percent int NOT NULL DEFAULT 70,
  audience text NOT NULL DEFAULT 'off', teams text[] NOT NULL DEFAULT '{}',
  updated_at timestamptz NOT NULL DEFAULT now(), updated_by text);
ALTER TABLE exams ADD COLUMN IF NOT EXISTS cert text;
CREATE TABLE IF NOT EXISTS exam_sources (
  exam_id text REFERENCES exams ON DELETE CASCADE, bank text, questions int, skipped int,
  sha256 text, uploaded_at timestamptz NOT NULL DEFAULT now(), uploaded_by text,
  PRIMARY KEY (exam_id, bank));
CREATE TABLE IF NOT EXISTS exam_cv (
  cvid uuid PRIMARY KEY, exam_id text REFERENCES exams ON DELETE CASCADE, bank text NOT NULL,
  cid text NOT NULL, title text NOT NULL, content text NOT NULL, hash text NOT NULL,
  UNIQUE (exam_id, bank, cid, hash));
CREATE TABLE IF NOT EXISTS exam_qv (
  vid uuid PRIMARY KEY, exam_id text REFERENCES exams ON DELETE CASCADE, bank text NOT NULL,
  qid text NOT NULL, version int NOT NULL, hash text NOT NULL, current boolean NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(), retired_at timestamptz,
  question text NOT NULL, qtype text NOT NULL, options jsonb NOT NULL DEFAULT '[]', answer text[] NOT NULL DEFAULT '{}',
  slots jsonb, case_cv uuid REFERENCES exam_cv, explanation text, refs jsonb NOT NULL DEFAULT '[]', domain text);
CREATE UNIQUE INDEX IF NOT EXISTS exam_qv_current ON exam_qv (exam_id, bank, qid) WHERE current;
CREATE INDEX IF NOT EXISTS exam_qv_key ON exam_qv (exam_id, bank, qid);
CREATE TABLE IF NOT EXISTS exam_attempts (
  id bigserial PRIMARY KEY, exam_id text REFERENCES exams ON DELETE CASCADE, member text NOT NULL,
  vids uuid[] NOT NULL, answers jsonb NOT NULL DEFAULT '{}', position int NOT NULL DEFAULT 0,
  started_at timestamptz NOT NULL DEFAULT now(), finished_at timestamptz,
  correct int, total int, score int, passed boolean, by_domain jsonb);
ALTER TABLE exam_attempts ADD COLUMN IF NOT EXISTS vids uuid[];
CREATE UNIQUE INDEX IF NOT EXISTS exam_attempts_open ON exam_attempts (exam_id, member) WHERE finished_at IS NULL;
CREATE INDEX IF NOT EXISTS exam_attempts_member ON exam_attempts (member, exam_id);
CREATE TABLE IF NOT EXISTS exam_uploads (
  id bigserial PRIMARY KEY, exam_id text REFERENCES exams ON DELETE CASCADE, bank text NOT NULL, filename text,
  uploaded_at timestamptz NOT NULL DEFAULT now(), uploaded_by text, questions int, new int, changed int,
  removed int, unchanged int, skipped int);
ALTER TABLE exam_uploads ADD COLUMN IF NOT EXISTS via text;
CREATE TABLE IF NOT EXISTS portal_meta (key text PRIMARY KEY, value text);
"""


class ExamError(ValueError):
    pass


def _conn():
    global _ready
    import psycopg2
    import psycopg2.extras
    psycopg2.extras.register_uuid()
    c = psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    if not _ready:
        with _lock:
            if not _ready:
                with c, c.cursor() as cur:
                    cur.execute("SELECT pg_advisory_xact_lock(4711)")   # one home-api at a time
                    cur.execute(SCHEMA)
                    _migrate_v1(cur)
                    cur.execute("INSERT INTO exam_uploads (exam_id, bank, uploaded_at, uploaded_by, questions, skipped) "
                                "SELECT exam_id, bank, uploaded_at, uploaded_by, questions, skipped FROM exam_sources "
                                "WHERE NOT EXISTS (SELECT 1 FROM exam_uploads)")   # history starts with what is there
                _ready = True
                try:
                    _seed_demo()
                except Exception as e:  # noqa: BLE001 — the portal works without the demo
                    print(f"[exams] demo exam not added: {e}", flush=True)
    return c


def _migrate_v1(cur):
    """The first layout (exam_banks / exam_questions / exam_cases, one row per question, attempts by
    qid) → banks, versions and attempts by vid. Once, in the schema transaction."""
    cur.execute("SELECT to_regclass('exam_questions') AS t")
    if not cur.fetchone()["t"]:
        return
    cur.execute("INSERT INTO exam_sources (exam_id, bank, questions, skipped, sha256, uploaded_at, uploaded_by) "
                "SELECT exam_id, exam_id || '/' || source, questions, skipped, sha256, uploaded_at, uploaded_by FROM exam_banks "
                "ON CONFLICT DO NOTHING")
    cur.execute("UPDATE exams SET cert = id WHERE cert IS NULL")
    cases = {}
    cur.execute("SELECT to_regclass('exam_cases') AS t")
    if cur.fetchone()["t"]:
        cur.execute("SELECT * FROM exam_cases")
        for c in cur.fetchall():
            bank, cvid = c["exam_id"] + "/" + c["source"], uuid.uuid4()
            cur.execute("INSERT INTO exam_cv (cvid, exam_id, bank, cid, title, content, hash) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                        (cvid, c["exam_id"], bank, c["cid"], c["title"], c["content"], _case_hash(c["title"], c["content"])))
            cases[(c["exam_id"], c["source"], c["cid"])] = (cvid, _case_hash(c["title"], c["content"]))
    vid_of = {}
    cur.execute("SELECT * FROM exam_questions")
    for q in cur.fetchall():
        case = cases.get((q["exam_id"], q["source"], q.get("case_study"))) if q.get("case_study") else None
        row = {"question": q["question"], "qtype": q.get("qtype") or "choice", "options": q["options"] or [],
               "answer": q["answer"] or [], "slots": q.get("slots"), "explanation": q["explanation"] or "",
               "refs": q["refs"] or [], "domain": q["domain"], "case_hash": case[1] if case else None}
        vid = uuid.uuid4()
        vid_of[(q["exam_id"], q["qid"])] = str(vid)
        cur.execute("INSERT INTO exam_qv (vid, exam_id, bank, qid, version, hash, current, question, qtype, options, answer, "
                    "slots, case_cv, explanation, refs, domain) VALUES (%s, %s, %s, %s, 1, %s, true, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (vid, q["exam_id"], q["exam_id"] + "/" + q["source"], q["qid"], _hash(row), row["question"], row["qtype"],
                     json.dumps(row["options"]), row["answer"], json.dumps(row["slots"]) if row["slots"] else None,
                     case[0] if case else None, row["explanation"], json.dumps(row["refs"]), row["domain"]))
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'exam_attempts' AND column_name = 'qids'")
    if cur.fetchone():
        cur.execute("SELECT id, exam_id, qids, answers FROM exam_attempts")
        for a in cur.fetchall():                  # questions deleted before (old re-uploads) are gone already
            vids = [vid_of[(a["exam_id"], q)] for q in a["qids"] if (a["exam_id"], q) in vid_of]
            answers = {vid_of[(a["exam_id"], q)]: v for q, v in (a["answers"] or {}).items() if (a["exam_id"], q) in vid_of}
            cur.execute("UPDATE exam_attempts SET vids = %s::uuid[], answers = %s WHERE id = %s", (vids, json.dumps(answers), a["id"]))
        cur.execute("ALTER TABLE exam_attempts DROP COLUMN qids")
    cur.execute("ALTER TABLE exam_attempts ALTER COLUMN vids SET NOT NULL")
    cur.execute("DROP TABLE IF EXISTS exam_cases, exam_questions, exam_banks")
    print(f"[exams] moved to question versions: {len(vid_of)} questions, {len(cases)} case studies", flush=True)


def _seed_demo():
    """The demo exam, once (portal_meta remembers it — a manager may delete it for good)."""
    if not os.path.exists(DEMO) or _q("SELECT 1 AS x FROM portal_meta WHERE key = 'demo_exam'", one=True):
        return
    with open(DEMO, "rb") as f:
        raw = f.read()
    demo = json.loads(raw)
    exam = upload_meta(demo["exam"], "lab")["exam"]
    _store_bank(demo["bank"], hashlib.sha256(raw).hexdigest(), "lab", exam)
    _q("UPDATE exams SET audience = 'all' WHERE id = %s", (exam,), rows=False)
    _q("INSERT INTO portal_meta (key, value) VALUES ('demo_exam', %s)", (exam,), rows=False)


def _q(sql, args=(), one=False, rows=True):
    c = _conn()
    try:
        with c, c.cursor() as cur:
            cur.execute(sql, args)
            if not rows:
                return cur.rowcount
            out = cur.fetchall() if cur.description else []
            return (out[0] if out else None) if one else out
    finally:
        c.close()


# ---- upload (managers) ---------------------------------------------------------------------
def decrypt(envelope, key_text):
    """The question bank (dict) inside an encrypted upload. Key: base64 of 32 bytes; its key_id
    (first 16 hex of sha256) must match; AES-256-GCM with the header string as AAD; gunzip;
    sha256 must match the header's."""
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    if envelope.get("format") != FORMAT or envelope.get("version") != 1:
        raise ExamError("not an encrypted question bank (unknown format or version)")
    t = (key_text or "").strip()
    try:
        key = base64.b64decode(t.replace("-", "+").replace("_", "/") + "=" * (-len(t) % 4))
    except Exception:
        raise ExamError("the key isn't valid base64") from None
    if len(key) != 32:
        raise ExamError("the key must be 32 bytes (base64, as in the .key file)")
    if hashlib.sha256(key).hexdigest()[:16] != envelope.get("key_id"):
        raise ExamError("wrong key for this file")
    try:
        raw = gzip.decompress(AESGCM(key).decrypt(base64.b64decode(envelope["nonce"]),
                                                  base64.b64decode(envelope["ciphertext"]),
                                                  envelope["header"].encode("utf-8")))
    except Exception:
        raise ExamError("could not decrypt: the file was changed or the key is wrong") from None
    header = json.loads(envelope["header"])
    if hashlib.sha256(raw).hexdigest() != header.get("sha256"):
        raise ExamError("checksum mismatch after decrypting")
    return json.loads(raw)


def _question(q, cases):
    """(stored fields) of one bank question, or None if it isn't usable (needs-fix, broken, an
    unknown type, or a case study the bank doesn't have)."""
    review = q.get("review") or {}
    corrected = review.get("status") == "corrected"
    qtype = q.get("type") or "choice"
    if review.get("status") == "needs-fix" or not q.get("question") or not q.get("id") \
            or (q.get("case_study") and q["case_study"] not in cases):
        return None
    row = {"qid": q["id"], "question": q["question"], "qtype": qtype, "options": [], "answer": [], "slots": None,
           "case_study": q.get("case_study"), "domain": q.get("domain"),
           "explanation": review.get("explanation") or q.get("explanation") or "",
           "refs": review.get("references") or q.get("references") or []}
    if qtype in SLOT_TYPES:
        fixed = (review.get("slot_answers") if corrected else None) or {}
        row["slots"] = []
        for s in q.get("slots") or []:
            choices = s.get("choices") or q.get("choice_pool") or (["Yes", "No"] if qtype == "yes-no" else [])
            answer = fixed.get(s.get("id")) or s.get("answer")
            if not s.get("id") or len(choices) < 2 or not answer or not set(answer) <= set(choices):
                return None
            row["slots"].append({"id": s["id"], "label": s.get("label") or "", "choices": list(choices), "answer": list(answer)})
        return row if row["slots"] else None
    if qtype != "choice":
        return None
    answer = review.get("answer") if corrected and review.get("answer") else q.get("answer")
    keys = [o.get("key") for o in q.get("options") or []]
    if len(keys) < 2 or not answer or not set(answer) <= set(keys):
        return None
    row["options"] = [{"key": o["key"], "text": o["text"]} for o in q["options"]]
    row["answer"] = sorted(answer)
    return row


def _case_hash(title, content):
    return hashlib.sha256(json.dumps([title, content]).encode()).hexdigest()


def _hash(r):
    """What a member sees and is marked on — a change makes a new version."""
    return hashlib.sha256(json.dumps({k: r.get(k) for k in ("question", "qtype", "options", "answer", "slots",
                                                            "explanation", "refs", "domain", "case_hash")},
                                     sort_keys=True).encode()).hexdigest()


def _slug(name):
    base = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")[:60].strip("-") or "exam"
    taken = {r["id"] for r in _q("SELECT id FROM exams WHERE id = %s OR id LIKE %s", (base, base + "-%"))}
    n, slug = 1, base
    while slug in taken:
        n += 1
        slug = f"{base}-{n}"
    return slug


def create_exam(name, by, cert=None, provider=None, domains=None, num_questions=None):
    """A new, empty exam (off until a manager opens it) → its id."""
    name = (name or "").strip()
    if not name or len(name) > 120:
        raise ExamError("name: 1–120 characters")
    exam_id = _slug(name)
    _q("INSERT INTO exams (id, name, cert, provider, domains, num_questions, updated_by) VALUES (%s, %s, %s, %s, %s, %s, %s)",
       (exam_id, name, cert, provider, json.dumps(domains or []), int(num_questions or 60), by), rows=False)
    print(f"[exams] {by}: new exam {exam_id} ({name})", flush=True)
    return {"exam": exam_id, "name": name}


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def fetch_link(url):
    """(bytes, name shown in the history) of a plain bank behind a link: https, a public GitHub file
    (github.com/…/blob/… is read raw) or an S3 presigned URL — no other hosts, no redirects, only
    public addresses (the lab's own services are never reached). The name leaves out the query
    (a presigned URL's signature)."""
    u = urllib.parse.urlsplit((url or "").strip())
    host = (u.hostname or "").lower()
    if u.scheme != "https" or not host:
        raise ExamError("the link must start with https://")
    path = u.path
    if host == "github.com":                              # …/owner/repo/blob/ref/path → the raw file
        m = re.fullmatch(r"/([^/]+)/([^/]+)/(?:blob|raw)/(.+)", path)
        if not m:
            raise ExamError("a GitHub link must point to a file (…/blob/<branch>/<path>)")
        host, path = "raw.githubusercontent.com", f"/{m[1]}/{m[2]}/{m[3]}"
    if host not in GITHUB_HOSTS and not S3_HOST.match(host):
        raise ExamError("only links to a public GitHub file or an AWS S3 presigned URL are accepted")
    try:
        addrs = {a[4][0] for a in socket.getaddrinfo(host, 443, proto=socket.IPPROTO_TCP)}
    except OSError:
        raise ExamError(f"can't find {host}") from None
    if not addrs or not all(ipaddress.ip_address(a).is_global for a in addrs):
        raise ExamError(f"{host} isn't a public address")
    link = urllib.parse.urlunsplit(("https", host, path, u.query, ""))
    try:
        with urllib.request.build_opener(_NoRedirect).open(
                urllib.request.Request(link, headers={"User-Agent": "de-lab-portal"}), timeout=20) as r:
            raw = r.read(MAX_BYTES + 1)
    except urllib.error.HTTPError as e:
        hint = (" — the presigned link may have expired" if e.code in (400, 403) and S3_HOST.match(host) else
                " — it points elsewhere: use the file's own link (GitHub: Raw; S3: the presigned URL as given)" if 300 <= e.code < 400 else "")
        raise ExamError(f"the link answered HTTP {e.code}{hint}") from None
    except (urllib.error.URLError, OSError) as e:
        raise ExamError(f"could not download the link: {getattr(e, 'reason', e)}") from None
    if len(raw) > MAX_BYTES:
        raise ExamError(f"the file is larger than {MAX_BYTES >> 20} MB")
    return raw, host + path


def upload(by, exam_id, file=None, key=None, url=None, filename=None):
    """Add a question bank to an exam — an encrypted file + its key, a link (no key) or a plain
    file (no key) — or an exam.json's details. → what changed (_store_bank) or the details saved."""
    via = "link" if url else "encrypted" if isinstance(file, dict) and file.get("format") else "file"
    if url:
        raw, filename = fetch_link(url)
        try:
            doc = json.loads(raw)
        except ValueError:
            raise ExamError("the link isn't a JSON file") from None
    else:
        doc = file if isinstance(file, dict) else None
        if not doc:
            raise ExamError("choose a file")
        raw = json.dumps(doc, sort_keys=True).encode()
    if doc.get("format"):
        if not (key or "").strip():
            raise ExamError("this file is encrypted — it needs its key (Encrypted file)")
        bank, sha256 = decrypt(doc, key), json.loads(doc["header"]).get("sha256")
    elif "questions" in doc:
        bank, sha256 = doc, hashlib.sha256(raw).hexdigest()
    elif doc.get("domains") or doc.get("exam_format"):
        return dict(upload_meta(doc, by, exam_id), details=True)
    else:
        raise ExamError("not a question bank (no questions) — start from the template")
    return _store_bank(bank, sha256, by, exam_id, filename=filename, via=via)


def upload_bank(envelope, key_text, by, exam_id=None, new_name=None, dry_run=False, filename=None):
    """Decrypt one bank and compare it with what the exam has (dry_run: only that), else store it."""
    return _store_bank(decrypt(envelope, key_text), json.loads(envelope["header"]).get("sha256"), by,
                       exam_id, new_name, dry_run, filename, "encrypted")


def _store_bank(bank, sha256, by, exam_id=None, new_name=None, dry_run=False, filename=None, via=None):
    """Add or update one bank in an exam (exam_id; or a new exam called new_name). → what changes."""
    cert, source = bank.get("exam_id", ""), bank.get("source", "")
    if not EXAM_ID.match(cert) or not EXAM_ID.match(source):
        raise ExamError("the bank has no valid exam_id / source")
    key = f"{cert}/{source}"
    cases = {c["id"]: c for c in bank.get("case_studies") or [] if c.get("id") and c.get("content")}
    rows = [r for r in (_question(q, cases) for q in bank.get("questions") or []) if r]
    if len({r["qid"] for r in rows}) != len(rows):
        raise ExamError("the bank has the same question id twice")
    skipped = len(bank.get("questions") or []) - len(rows)
    if not rows:
        raise ExamError("no usable questions in this file")
    for r in rows:
        c = cases.get(r["case_study"])
        r["case_hash"] = _case_hash(c.get("title") or c["id"], c["content"]) if c else None
        r["hash"] = _hash(r)
    e = _q("SELECT id, name FROM exams WHERE id = %s", (exam_id,), one=True) if exam_id else None
    if not e and not (new_name or "").strip():
        raise ExamError("pick the exam to add these questions to (or name a new one)")
    have = {r["qid"]: r for r in _q("SELECT qid, hash, version FROM exam_qv WHERE exam_id = %s AND bank = %s AND current",
                                    (e["id"], key))} if e else {}
    new = [r for r in rows if r["qid"] not in have]
    changed = [r for r in rows if r["qid"] in have and have[r["qid"]]["hash"] != r["hash"]]
    gone = sorted(set(have) - {r["qid"] for r in rows})
    summary = {"exam": e["id"] if e else None, "name": e["name"] if e else new_name.strip(), "bank": key,
               "replaces": bool(have), "new": len(new), "changed": len(changed), "removed": len(gone),
               "unchanged": len(rows) - len(new) - len(changed), "skipped": skipped,
               "changed_ids": [r["qid"] for r in changed][:50], "removed_ids": gone[:50]}
    if dry_run:
        return summary
    if not e:
        summary["exam"] = create_exam(new_name, by, cert=cert)["exam"]
    exam_id = summary["exam"]
    import psycopg2.extras
    c = _conn()
    try:
        with c, c.cursor() as cur:
            cur.execute("INSERT INTO exam_sources (exam_id, bank, questions, skipped, sha256, uploaded_by) VALUES (%s, %s, %s, %s, %s, %s) "
                        "ON CONFLICT (exam_id, bank) DO UPDATE SET questions = EXCLUDED.questions, skipped = EXCLUDED.skipped, "
                        "sha256 = EXCLUDED.sha256, uploaded_at = now(), uploaded_by = EXCLUDED.uploaded_by",
                        (exam_id, key, len(rows), skipped, sha256, by))
            cvid = {}
            for cid in {r["case_study"] for r in new + changed if r["case_study"]}:
                cs = cases[cid]
                title, h = cs.get("title") or cid, _case_hash(cs.get("title") or cid, cs["content"])
                cur.execute("SELECT cvid FROM exam_cv WHERE exam_id = %s AND bank = %s AND cid = %s AND hash = %s",
                            (exam_id, key, cid, h))
                hit = cur.fetchone()
                cvid[cid] = hit["cvid"] if hit else uuid.uuid4()
                if not hit:
                    cur.execute("INSERT INTO exam_cv (cvid, exam_id, bank, cid, title, content, hash) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                                (cvid[cid], exam_id, key, cid, title, cs["content"], h))
            retire = [r["qid"] for r in changed] + gone
            if retire:
                cur.execute("UPDATE exam_qv SET current = false, retired_at = now() WHERE exam_id = %s AND bank = %s "
                            "AND qid = ANY(%s) AND current", (exam_id, key, retire))
            if new or changed:
                psycopg2.extras.execute_values(cur, "INSERT INTO exam_qv (vid, exam_id, bank, qid, version, hash, current, question, "
                                               "qtype, options, answer, slots, case_cv, explanation, refs, domain) VALUES %s",
                                               [(uuid.uuid4(), exam_id, key, r["qid"], have[r["qid"]]["version"] + 1 if r["qid"] in have else 1,
                                                 r["hash"], True, r["question"], r["qtype"], json.dumps(r["options"]), r["answer"],
                                                 json.dumps(r["slots"]) if r["slots"] else None, cvid.get(r["case_study"]),
                                                 r["explanation"], json.dumps(r["refs"]), r["domain"])
                                                for r in new + changed])
            cur.execute("INSERT INTO exam_uploads (exam_id, bank, filename, via, uploaded_by, questions, new, changed, removed, "
                        "unchanged, skipped) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        (exam_id, key, (filename or "")[:300] or None, via, by, len(rows), len(new), len(changed), len(gone),
                         summary["unchanged"], skipped))
    finally:
        c.close()
    print(f"[exams] {by}: {exam_id} ← {key}: {len(new)} new, {len(changed)} changed, {len(gone)} removed, "
          f"{summary['unchanged']} unchanged ({skipped} left out)", flush=True)
    return dict(summary, pool=pool_size(exam_id))


def upload_meta(meta, by, exam_id=None):
    """A certification's exam.json (plain): provider and domains for the exam picked (or the exam made
    from it before) — a new exam with its name and question count when there is none."""
    cert = meta.get("id", "")
    if not EXAM_ID.match(cert) or not meta.get("name"):
        raise ExamError("not an exam.json (needs id and name)")
    domains = [{"id": str(d.get("id")), "name": d.get("name", ""), "weight": d.get("weight_percent")}
               for d in meta.get("domains") or []]
    count = int((meta.get("exam_format") or {}).get("question_count") or 60)
    e = _q("SELECT id, name FROM exams WHERE id = %s", (exam_id,), one=True) if exam_id else \
        _q("SELECT id, name FROM exams WHERE cert = %s OR id = %s ORDER BY (id = %s) DESC LIMIT 1", (cert, cert, cert), one=True)
    if not e:
        r = create_exam(meta["name"], by, cert=cert, provider=meta.get("provider"), domains=domains, num_questions=count)
        return dict(r, domains=len(domains), created=True)
    _q("UPDATE exams SET provider = %s, domains = %s, cert = coalesce(cert, %s), updated_at = now(), updated_by = %s WHERE id = %s",
       (meta.get("provider"), json.dumps(domains), cert, by, e["id"]), rows=False)
    return {"exam": e["id"], "name": e["name"], "domains": len(domains), "created": False}


def pool_size(exam_id):
    return _q("SELECT count(*) AS n FROM exam_qv WHERE exam_id = %s AND current", (exam_id,), one=True)["n"]


def set_exam(exam_id, by, name=None, num_questions=None, pass_percent=None, audience=None, teams=None):
    e = _q("SELECT * FROM exams WHERE id = %s", (exam_id,), one=True)
    if not e:
        raise ExamError(f"no exam {exam_id}")
    name = (name if name is not None else e["name"]).strip()
    n = int(num_questions if num_questions is not None else e["num_questions"])
    p = int(pass_percent if pass_percent is not None else e["pass_percent"])
    a = audience if audience is not None else e["audience"]
    t = sorted(set(teams if teams is not None else e["teams"]))
    if not name or len(name) > 120:
        raise ExamError("name: 1–120 characters")
    if not 1 <= n <= 500:
        raise ExamError("number of questions: 1–500")
    if not 1 <= p <= 100:
        raise ExamError("pass mark: 1–100 %")
    if a not in AUDIENCES:
        raise ExamError("audience: all, teams or off")
    if a == "teams" and not t:
        raise ExamError("pick at least one team (or choose All members)")
    _q("UPDATE exams SET name=%s, num_questions=%s, pass_percent=%s, audience=%s, teams=%s, updated_at=now(), "
       "updated_by=%s WHERE id=%s", (name, n, p, a, t, by, exam_id), rows=False)
    return admin_exams()


def delete_bank(exam_id, bank):
    """A bank leaves the exam: its questions are retired (old reviews keep them)."""
    _q("UPDATE exam_qv SET current = false, retired_at = now() WHERE exam_id = %s AND bank = %s AND current",
       (exam_id, bank), rows=False)
    return {"deleted": _q("DELETE FROM exam_sources WHERE exam_id = %s AND bank = %s", (exam_id, bank), rows=False)}


def delete_exam(exam_id):
    """The exam, its questions and every attempt."""
    return {"deleted": _q("DELETE FROM exams WHERE id = %s", (exam_id,), rows=False)}


def discard(exam_id, attempt_id=None):
    """Delete an attempt in progress (or all of the exam's): the member starts afresh."""
    if attempt_id is not None:
        n = _q("DELETE FROM exam_attempts WHERE id = %s AND exam_id = %s AND finished_at IS NULL",
               (int(attempt_id), exam_id), rows=False)
    else:
        n = _q("DELETE FROM exam_attempts WHERE exam_id = %s AND finished_at IS NULL", (exam_id,), rows=False)
    return {"deleted": n}


def results(exam_id):
    """Managers: each member's numbers for the exam, and the latest submitted attempts."""
    members = _q("SELECT member, count(*) AS attempts, max(score) AS best, round(avg(score)) AS average, "
                 "sum(CASE WHEN passed THEN 1 ELSE 0 END) AS passed, max(finished_at) AS last "
                 "FROM exam_attempts WHERE exam_id = %s AND finished_at IS NOT NULL GROUP BY member ORDER BY member",
                 (exam_id,))
    recent = _q("SELECT id, member, score, passed, correct, total, started_at, finished_at FROM exam_attempts "
                "WHERE exam_id = %s AND finished_at IS NOT NULL ORDER BY finished_at DESC LIMIT 100", (exam_id,))
    return {"members": [dict(m, average=int(m["average"]), last=m["last"].isoformat()) for m in members],
            "recent": [{"id": a["id"], "member": a["member"], "score": a["score"], "passed": a["passed"],
                        "correct": a["correct"], "total": a["total"], "finished_at": a["finished_at"].isoformat(),
                        "minutes": max(1, round((a["finished_at"] - a["started_at"]).total_seconds() / 60))}
                       for a in recent]}


def admin_questions(exam_id):
    """Managers: every current question of the exam with its right answer and explanation (bank, id order)."""
    qs = _q("SELECT * FROM exam_qv WHERE exam_id = %s AND current ORDER BY bank, qid", (exam_id,))
    out = []
    for q in qs:
        d = dict(_shown(q), domain=q["domain"], explanation=q["explanation"], refs=q["refs"],
                 version=q["version"], since=q["created_at"].isoformat())
        if q["qtype"] in SLOT_TYPES:
            d["slots"] = [{"id": s["id"], "label": s["label"], "choices": s["choices"], "answer": s["answer"]} for s in q["slots"]]
        else:
            d["answer"] = q["answer"]
        out.append(d)
    return {"cases": _cases(qs), "questions": out}


def admin_exams():
    """Every exam with its banks, pool and attempt numbers (managers)."""
    exams = _q("SELECT e.*, (SELECT count(*) FROM exam_qv q WHERE q.exam_id = e.id AND current) AS pool, "
               "(SELECT count(*) FROM exam_attempts a WHERE a.exam_id = e.id AND finished_at IS NOT NULL) AS attempts, "
               "(SELECT count(DISTINCT member) FROM exam_attempts a WHERE a.exam_id = e.id) AS members, "
               "(SELECT round(avg(score)) FROM exam_attempts a WHERE a.exam_id = e.id AND finished_at IS NOT NULL) AS avg_score "
               "FROM exams e ORDER BY e.name")
    banks = _q("SELECT s.exam_id, s.bank, s.questions, s.skipped, s.uploaded_at, s.uploaded_by, "
               "(SELECT count(*) FROM exam_qv q WHERE q.exam_id = s.exam_id AND q.bank = s.bank AND q.version > 1 AND current) AS updated "
               "FROM exam_sources s ORDER BY s.bank")
    ongoing = _q("SELECT id, exam_id, member, started_at, cardinality(vids) AS total, "
                 "(SELECT count(*) FROM jsonb_object_keys(answers)) AS answered "
                 "FROM exam_attempts WHERE finished_at IS NULL ORDER BY started_at")
    uploads = _q("SELECT * FROM exam_uploads ORDER BY uploaded_at DESC, id DESC")
    for e in exams:
        e["uploads"] = [dict(u, uploaded_at=u["uploaded_at"].isoformat()) for u in uploads if u["exam_id"] == e["id"]][:100]
        e["banks"] = [dict(b, uploaded_at=b["uploaded_at"].isoformat()) for b in banks if b["exam_id"] == e["id"]]
        e["ongoing"] = [{"id": a["id"], "member": a["member"], "started_at": a["started_at"].isoformat(),
                         "answered": a["answered"], "total": a["total"]} for a in ongoing if a["exam_id"] == e["id"]]
        e["updated_at"] = e["updated_at"].isoformat()
        e["avg_score"] = int(e["avg_score"]) if e["avg_score"] is not None else None
    return {"exams": exams}


# ---- taking an exam (members) --------------------------------------------------------------
def _allowed(e, team):
    return e["audience"] == "all" or (e["audience"] == "teams" and team in (e["teams"] or []))


def _exam_for(exam_id, team):
    e = _q("SELECT * FROM exams WHERE id = %s", (exam_id,), one=True)
    if not e or not _allowed(e, team):
        raise PermissionError("this exam isn't open to you")
    return e


def my_exams(member, team):
    """The exams open to the member, each with their stats and any open (paused) attempt."""
    out = []
    for e in _q("SELECT e.*, (SELECT count(*) FROM exam_qv q WHERE q.exam_id = e.id AND current) AS pool "
                "FROM exams e WHERE audience <> 'off' ORDER BY name"):
        if not _allowed(e, team) or not e["pool"]:
            continue
        done = _q("SELECT id, score, passed, correct, total, started_at, finished_at FROM exam_attempts "
                  "WHERE exam_id = %s AND member = %s AND finished_at IS NOT NULL ORDER BY finished_at DESC",
                  (e["id"], member))
        open_ = _q("SELECT id, vids, answers, position, started_at FROM exam_attempts "
                   "WHERE exam_id = %s AND member = %s AND finished_at IS NULL", (e["id"], member), one=True)
        scores = [a["score"] for a in done]
        out.append({"id": e["id"], "name": e["name"], "provider": e["provider"],
                    "questions": min(e["num_questions"], e["pool"]), "pass_percent": e["pass_percent"],
                    "stats": {"attempts": len(done), "passed": sum(1 for a in done if a["passed"]),
                              "best": max(scores) if scores else None, "last": scores[0] if scores else None,
                              "average": round(sum(scores) / len(scores)) if scores else None},
                    "history": [{"id": a["id"], "score": a["score"], "passed": a["passed"], "correct": a["correct"],
                                 "total": a["total"], "finished_at": a["finished_at"].isoformat(),
                                 "minutes": max(1, round((a["finished_at"] - a["started_at"]).total_seconds() / 60))}
                                for a in done[:20]],
                    "open": {"id": open_["id"], "answered": len(open_["answers"]), "total": len(open_["vids"]),
                             "started_at": open_["started_at"].isoformat()} if open_ else None})
    return {"exams": out}


def start(exam_id, member, team):
    """Resume the member's open attempt of the exam, or draw a new set of questions (current versions;
    least seen first — seen counts a question in any version)."""
    e = _exam_for(exam_id, team)
    open_ = _q("SELECT id FROM exam_attempts WHERE exam_id = %s AND member = %s AND finished_at IS NULL",
               (exam_id, member), one=True)
    if open_:
        return attempt(open_["id"], member, team)
    pool = _q("SELECT vid, bank, qid FROM exam_qv WHERE exam_id = %s AND current", (exam_id,))
    if not pool:
        raise ExamError("this exam has no questions yet")
    seen = {}
    for r in _q("SELECT q.bank, q.qid FROM exam_attempts a CROSS JOIN LATERAL unnest(a.vids) AS v(vid) "
                "JOIN exam_qv q ON q.vid = v.vid WHERE a.exam_id = %s AND a.member = %s", (exam_id, member)):
        seen[(r["bank"], r["qid"])] = seen.get((r["bank"], r["qid"]), 0) + 1
    random.shuffle(pool)                                  # least seen first, random among equals
    pick = sorted(pool, key=lambda q: seen.get((q["bank"], q["qid"]), 0))[:min(e["num_questions"], len(pool))]
    random.shuffle(pick)
    row = _q("INSERT INTO exam_attempts (exam_id, member, vids) VALUES (%s, %s, %s::uuid[]) RETURNING id",
             (exam_id, member, [str(q["vid"]) for q in pick]), one=True)
    return attempt(row["id"], member, team)


def _own(attempt_id, member):
    a = _q("SELECT * FROM exam_attempts WHERE id = %s AND member = %s", (int(attempt_id), member), one=True)
    if not a:
        raise PermissionError("no such attempt")
    a["vids"] = [str(v) for v in a["vids"]]
    return a


def _questions(vids):
    """The question versions, in the attempt's order."""
    by = {str(r["vid"]): r for r in _q("SELECT * FROM exam_qv WHERE vid = ANY(%s::uuid[])", (list(vids),))}
    return [by[v] for v in vids if v in by]


def attempt(attempt_id, member, team):
    """An open attempt to work on: its questions WITHOUT answers, the member's picks, position."""
    a = _own(attempt_id, member)
    if a["finished_at"]:
        return review(attempt_id, member)
    e = _exam_for(a["exam_id"], team)
    qs = _questions(a["vids"])
    return {"id": a["id"], "exam": {"id": e["id"], "name": e["name"], "pass_percent": e["pass_percent"]},
            "done": False, "position": a["position"], "answers": a["answers"], "cases": _cases(qs),
            "questions": [_shown(q) for q in qs]}


def _cases(qs):
    """The case studies of these questions: {cvid: {title, content}} — once, not per question."""
    ids = list({str(q["case_cv"]) for q in qs if q.get("case_cv")})
    if not ids:
        return {}
    return {str(r["cvid"]): {"title": r["title"], "content": r["content"]}
            for r in _q("SELECT cvid, title, content FROM exam_cv WHERE cvid = ANY(%s::uuid[])", (ids,))}


def _shown(q):
    """A question as the member sees it while answering — no answers. qid is the version's id (the
    answers' key); ref is the question's own id in its bank."""
    d = {"qid": str(q["vid"]), "ref": q["qid"], "bank": q["bank"], "type": q["qtype"], "question": q["question"],
         "case": str(q["case_cv"]) if q.get("case_cv") else None}
    if q["qtype"] in SLOT_TYPES:
        d["slots"] = [{"id": s["id"], "label": s["label"], "choices": s["choices"]} for s in q["slots"]]
    else:
        d["options"], d["choose"] = q["options"], len(q["answer"])
    return d


def _mark(q, given):
    """(points 0…1, per-slot right/wrong or None): a choice question is right only with exactly the
    right keys; a slot question gets a share of its point for each slot that is right."""
    if q["qtype"] in SLOT_TYPES:
        given = given if isinstance(given, dict) else {}
        oks = [given.get(s["id"]) in s["answer"] for s in q["slots"]]
        return sum(oks) / len(oks), oks
    return float(isinstance(given, list) and sorted(given) == sorted(q["answer"])), None


def answer(attempt_id, member, qid, keys, position=None, slots=None):
    """Save one answer — option keys, or {slot id: value} for a slot question — and where the
    member is: leaving now is a pause. qid = the question version's id."""
    a = _own(attempt_id, member)
    if a["finished_at"]:
        raise ExamError("this attempt is already submitted")
    if qid not in a["vids"]:
        raise ExamError("that question isn't in this attempt")
    q = (_questions([qid]) or [None])[0]
    if q and q["qtype"] in SLOT_TYPES:
        choices = {s["id"]: s["choices"] for s in q["slots"]}
        value = {k: v for k, v in (slots if isinstance(slots, dict) else {}).items()
                 if k in choices and isinstance(v, str) and v in choices[k]}
    else:
        value = sorted({k for k in keys or [] if isinstance(k, str) and re.fullmatch(r"[A-Z]", k)})
    answers = dict(a["answers"])
    if value:
        answers[qid] = value
    else:
        answers.pop(qid, None)
    pos = a["position"] if position is None else max(0, min(int(position), len(a["vids"]) - 1))
    _q("UPDATE exam_attempts SET answers = %s, position = %s WHERE id = %s", (json.dumps(answers), pos, a["id"]),
       rows=False)
    return {"saved": qid, "answered": len(answers)}


def submit(attempt_id, member, team):
    """Mark the attempt (_mark: whole points, slot questions in shares) → the review."""
    a = _own(attempt_id, member)
    if a["finished_at"]:
        return review(attempt_id, member)
    e = _q("SELECT * FROM exams WHERE id = %s", (a["exam_id"],), one=True)
    qs = _questions(a["vids"])
    right, points, by_domain = 0, 0.0, {}
    for q in qs:
        p, _ = _mark(q, a["answers"].get(str(q["vid"])))
        right += p == 1
        points += p
        d = (q["domain"] or "").split(".")[0] or "-"
        by_domain.setdefault(d, [0, 0])
        by_domain[d][0] = round(by_domain[d][0] + p, 2)
        by_domain[d][1] += 1
    total = len(qs) or 1
    score = round(100 * points / total)
    _q("UPDATE exam_attempts SET finished_at = now(), correct = %s, total = %s, score = %s, passed = %s, "
       "by_domain = %s WHERE id = %s",
       (right, len(qs), score, score >= e["pass_percent"], json.dumps(by_domain), a["id"]), rows=False)
    return review(attempt_id, member)


def review(attempt_id, member):
    """A submitted attempt: score, pass/fail, per domain, every question (the versions it was marked
    on) with the member's answer, the right one and the explanation."""
    a = _own(attempt_id, member)
    if not a["finished_at"]:
        raise ExamError("submit the attempt first")
    e = _q("SELECT * FROM exams WHERE id = %s", (a["exam_id"],), one=True)
    names = {d["id"]: d["name"] for d in (e["domains"] or [])}
    qs = _questions(a["vids"])
    return {"id": a["id"], "done": True,
            "exam": {"id": e["id"], "name": e["name"], "pass_percent": e["pass_percent"]},
            "score": a["score"], "passed": a["passed"], "correct": a["correct"], "total": a["total"],
            "finished_at": a["finished_at"].isoformat(),
            "minutes": max(1, round((a["finished_at"] - a["started_at"]).total_seconds() / 60)),
            "by_domain": [{"domain": d, "name": names.get(d, "Unassigned" if d == "-" else f"Domain {d}"),
                           "correct": v[0], "total": v[1]} for d, v in sorted((a["by_domain"] or {}).items())],
            "cases": _cases(qs), "questions": [_reviewed(q, a["answers"].get(str(q["vid"]))) for q in qs]}


def _reviewed(q, given):
    """A question in the review: the member's answer, the right one, points, the explanation."""
    p, oks = _mark(q, given)
    d = dict(_shown(q), points=round(p, 2), right=p == 1, answered=bool(given),
             explanation=q["explanation"], refs=q["refs"])
    if oks is None:
        d.update(answer=q["answer"], picked=given or [])
    else:
        given = given if isinstance(given, dict) else {}
        d["slots"] = [{"id": s["id"], "label": s["label"], "answer": s["answer"], "picked": given.get(s["id"]), "right": ok}
                      for s, ok in zip(q["slots"], oks)]
    return d


def forget_member(member):
    """A member is deleted: their attempts go too."""
    return _q("DELETE FROM exam_attempts WHERE member = %s", (member,), rows=False)
