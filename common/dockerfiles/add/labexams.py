"""labexams — mock certification exams on the portal (home-api), kept in the portal database.

Managers upload question banks they receive encrypted (certification-exams format, AES-256-GCM —
see decrypt()) together with the key; the key is used once and never stored. Each certification
is one exam (its exam_id); several banks (sources: dump1, dump2 …) add up to its question pool,
uploading a source again replaces it. An optional plain exam.json gives the name and domains.
Managers set how many questions an attempt has, the pass mark, and who may take it: everyone,
chosen teams, or nobody (off). They see the attempts in progress and can delete them (one or all
of an exam) — the member's next start then draws a new set with the current settings.

Members take an exam any number of times. Each attempt draws its questions at random, preferring
the ones the member has seen least; answers are saved as they go, so leaving is a pause and the
attempt resumes where it was. No time limit. After submitting: score, pass/fail, per domain and
a full review. Correct answers never reach the browser before the attempt is submitted.

Questions marked review.status = needs-fix are left out; review.answer / review.explanation win
over the source's when present.

DB: PORTAL_DB_URL (postgres://portal:…@postgres:5432/portal); tables are made on first use.
"""
import base64
import gzip
import hashlib
import json
import os
import random
import re
import threading

DB_URL = os.environ.get("PORTAL_DB_URL", "postgresql://portal:portal@postgres:5432/portal")
FORMAT = "certification-exams/question-bank+aes-256-gcm"
EXAM_ID = re.compile(r"^[a-z0-9]+(-[a-z0-9]+)*$")
AUDIENCES = ("all", "teams", "off")
PASS_PERCENT = 70
_ready = False
_lock = threading.Lock()

SCHEMA = """
CREATE TABLE IF NOT EXISTS exams (
  id text PRIMARY KEY, name text NOT NULL, provider text, domains jsonb NOT NULL DEFAULT '[]',
  num_questions int NOT NULL DEFAULT 60, pass_percent int NOT NULL DEFAULT 70,
  audience text NOT NULL DEFAULT 'off', teams text[] NOT NULL DEFAULT '{}',
  updated_at timestamptz NOT NULL DEFAULT now(), updated_by text);
CREATE TABLE IF NOT EXISTS exam_banks (
  exam_id text REFERENCES exams ON DELETE CASCADE, source text, questions int, skipped int,
  sha256 text, uploaded_at timestamptz NOT NULL DEFAULT now(), uploaded_by text,
  PRIMARY KEY (exam_id, source));
CREATE TABLE IF NOT EXISTS exam_questions (
  exam_id text, source text, qid text, question text NOT NULL, options jsonb NOT NULL,
  answer text[] NOT NULL, explanation text, refs jsonb NOT NULL DEFAULT '[]', domain text,
  PRIMARY KEY (exam_id, qid), FOREIGN KEY (exam_id, source) REFERENCES exam_banks ON DELETE CASCADE);
CREATE TABLE IF NOT EXISTS exam_attempts (
  id bigserial PRIMARY KEY, exam_id text REFERENCES exams ON DELETE CASCADE, member text NOT NULL,
  qids text[] NOT NULL, answers jsonb NOT NULL DEFAULT '{}', position int NOT NULL DEFAULT 0,
  started_at timestamptz NOT NULL DEFAULT now(), finished_at timestamptz,
  correct int, total int, score int, passed boolean, by_domain jsonb);
CREATE UNIQUE INDEX IF NOT EXISTS exam_attempts_open ON exam_attempts (exam_id, member) WHERE finished_at IS NULL;
CREATE INDEX IF NOT EXISTS exam_attempts_member ON exam_attempts (member, exam_id);
"""


class ExamError(ValueError):
    pass


def _conn():
    global _ready
    import psycopg2
    import psycopg2.extras
    c = psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    if not _ready:
        with _lock, c, c.cursor() as cur:
            cur.execute(SCHEMA)
        _ready = True
    return c


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


def _question(q):
    """(stored fields) of one bank question, or None if it isn't usable (needs-fix, broken)."""
    review = q.get("review") or {}
    if review.get("status") == "needs-fix":
        return None
    answer = review.get("answer") if review.get("status") == "corrected" and review.get("answer") else q.get("answer")
    keys = [o.get("key") for o in q.get("options") or []]
    if not q.get("question") or len(keys) < 2 or not answer or not set(answer) <= set(keys):
        return None
    return {"qid": q["id"], "question": q["question"],
            "options": [{"key": o["key"], "text": o["text"]} for o in q["options"]],
            "answer": sorted(answer), "explanation": review.get("explanation") or q.get("explanation") or "",
            "refs": review.get("references") or q.get("references") or [], "domain": q.get("domain")}


def _title(exam_id):
    return " ".join(w.capitalize() for w in exam_id.split("-"))


def upload_bank(envelope, key_text, by):
    """Decrypt and store one bank (replacing that source). → summary."""
    bank = decrypt(envelope, key_text)
    exam_id, source = bank.get("exam_id", ""), bank.get("source", "")
    if not EXAM_ID.match(exam_id) or not EXAM_ID.match(source):
        raise ExamError("the bank has no valid exam_id / source")
    rows = [r for r in (_question(q) for q in bank.get("questions") or []) if r]
    skipped = len(bank.get("questions") or []) - len(rows)
    if not rows:
        raise ExamError("no usable questions in this file")
    import psycopg2.extras
    c = _conn()
    try:
        with c, c.cursor() as cur:
            cur.execute("INSERT INTO exams (id, name, updated_by) VALUES (%s, %s, %s) ON CONFLICT (id) DO NOTHING",
                        (exam_id, _title(exam_id), by))
            cur.execute("DELETE FROM exam_banks WHERE exam_id = %s AND source = %s", (exam_id, source))
            cur.execute("INSERT INTO exam_banks (exam_id, source, questions, skipped, sha256, uploaded_by) "
                        "VALUES (%s, %s, %s, %s, %s, %s)",
                        (exam_id, source, len(rows), skipped, json.loads(envelope["header"]).get("sha256"), by))
            cur.execute("DELETE FROM exam_questions WHERE exam_id = %s AND qid = ANY(%s)",
                        (exam_id, [r["qid"] for r in rows]))       # an id moved between sources
            psycopg2.extras.execute_values(cur, "INSERT INTO exam_questions (exam_id, source, qid, question, "
                                           "options, answer, explanation, refs, domain) VALUES %s",
                                           [(exam_id, source, r["qid"], r["question"], json.dumps(r["options"]),
                                             r["answer"], r["explanation"], json.dumps(r["refs"]), r["domain"])
                                            for r in rows])
    finally:
        c.close()
    print(f"[exams] {by}: {exam_id}/{source} — {len(rows)} questions ({skipped} skipped)", flush=True)
    return {"exam": exam_id, "source": source, "questions": len(rows), "skipped": skipped,
            "pool": pool_size(exam_id)}


def upload_meta(meta, by):
    """A certification's exam.json (plain): name, provider, domains, default question count."""
    exam_id = meta.get("id", "")
    if not EXAM_ID.match(exam_id) or not meta.get("name"):
        raise ExamError("not an exam.json (needs id and name)")
    domains = [{"id": str(d.get("id")), "name": d.get("name", ""), "weight": d.get("weight_percent")}
               for d in meta.get("domains") or []]
    count = int((meta.get("exam_format") or {}).get("question_count") or 60)
    _q("INSERT INTO exams (id, name, provider, domains, num_questions, updated_by) VALUES (%s, %s, %s, %s, %s, %s) "
       "ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, provider = EXCLUDED.provider, "
       "domains = EXCLUDED.domains, updated_at = now(), updated_by = EXCLUDED.updated_by",
       (exam_id, meta["name"], meta.get("provider"), json.dumps(domains), count, by), rows=False)
    return {"exam": exam_id, "name": meta["name"], "domains": len(domains)}


def pool_size(exam_id):
    return _q("SELECT count(*) AS n FROM exam_questions WHERE exam_id = %s", (exam_id,), one=True)["n"]


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


def delete_bank(exam_id, source):
    return {"deleted": _q("DELETE FROM exam_banks WHERE exam_id = %s AND source = %s", (exam_id, source), rows=False)}


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


def admin_exams():
    """Every exam with its banks, pool and attempt numbers (managers)."""
    exams = _q("SELECT e.*, (SELECT count(*) FROM exam_questions q WHERE q.exam_id = e.id) AS pool, "
               "(SELECT count(*) FROM exam_attempts a WHERE a.exam_id = e.id AND finished_at IS NOT NULL) AS attempts, "
               "(SELECT count(DISTINCT member) FROM exam_attempts a WHERE a.exam_id = e.id) AS members, "
               "(SELECT round(avg(score)) FROM exam_attempts a WHERE a.exam_id = e.id AND finished_at IS NOT NULL) AS avg_score "
               "FROM exams e ORDER BY e.name")
    banks = _q("SELECT exam_id, source, questions, skipped, uploaded_at, uploaded_by FROM exam_banks ORDER BY source")
    ongoing = _q("SELECT id, exam_id, member, started_at, cardinality(qids) AS total, "
                 "(SELECT count(*) FROM jsonb_object_keys(answers)) AS answered "
                 "FROM exam_attempts WHERE finished_at IS NULL ORDER BY started_at")
    for e in exams:
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
    for e in _q("SELECT e.*, (SELECT count(*) FROM exam_questions q WHERE q.exam_id = e.id) AS pool "
                "FROM exams e WHERE audience <> 'off' ORDER BY name"):
        if not _allowed(e, team) or not e["pool"]:
            continue
        done = _q("SELECT id, score, passed, correct, total, started_at, finished_at FROM exam_attempts "
                  "WHERE exam_id = %s AND member = %s AND finished_at IS NOT NULL ORDER BY finished_at DESC",
                  (e["id"], member))
        open_ = _q("SELECT id, qids, answers, position, started_at FROM exam_attempts "
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
                    "open": {"id": open_["id"], "answered": len(open_["answers"]), "total": len(open_["qids"]),
                             "started_at": open_["started_at"].isoformat()} if open_ else None})
    return {"exams": out}


def start(exam_id, member, team):
    """Resume the member's open attempt of the exam, or draw a new set of questions."""
    e = _exam_for(exam_id, team)
    open_ = _q("SELECT id FROM exam_attempts WHERE exam_id = %s AND member = %s AND finished_at IS NULL",
               (exam_id, member), one=True)
    if open_:
        return attempt(open_["id"], member, team)
    pool = [r["qid"] for r in _q("SELECT qid FROM exam_questions WHERE exam_id = %s", (exam_id,))]
    if not pool:
        raise ExamError("this exam has no questions yet")
    seen = {}
    for r in _q("SELECT unnest(qids) AS qid FROM exam_attempts WHERE exam_id = %s AND member = %s", (exam_id, member)):
        seen[r["qid"]] = seen.get(r["qid"], 0) + 1
    random.shuffle(pool)                                  # least seen first, random among equals
    pick = sorted(pool, key=lambda q: seen.get(q, 0))[:min(e["num_questions"], len(pool))]
    random.shuffle(pick)
    row = _q("INSERT INTO exam_attempts (exam_id, member, qids) VALUES (%s, %s, %s) RETURNING id",
             (exam_id, member, pick), one=True)
    return attempt(row["id"], member, team)


def _own(attempt_id, member):
    a = _q("SELECT * FROM exam_attempts WHERE id = %s AND member = %s", (int(attempt_id), member), one=True)
    if not a:
        raise PermissionError("no such attempt")
    return a


def _questions(exam_id, qids):
    by = {r["qid"]: r for r in _q("SELECT * FROM exam_questions WHERE exam_id = %s AND qid = ANY(%s)", (exam_id, qids))}
    return [by[q] for q in qids if q in by]


def attempt(attempt_id, member, team):
    """An open attempt to work on: its questions WITHOUT answers, the member's picks, position."""
    a = _own(attempt_id, member)
    if a["finished_at"]:
        return review(attempt_id, member)
    e = _exam_for(a["exam_id"], team)
    qs = _questions(a["exam_id"], a["qids"])
    return {"id": a["id"], "exam": {"id": e["id"], "name": e["name"], "pass_percent": e["pass_percent"]},
            "done": False, "position": a["position"], "answers": a["answers"],
            "questions": [{"qid": q["qid"], "question": q["question"], "options": q["options"],
                           "choose": len(q["answer"])} for q in qs]}


def answer(attempt_id, member, qid, keys, position=None):
    """Save one answer (and where the member is) — leaving now is a pause."""
    a = _own(attempt_id, member)
    if a["finished_at"]:
        raise ExamError("this attempt is already submitted")
    if qid not in a["qids"]:
        raise ExamError("that question isn't in this attempt")
    keys = sorted({k for k in keys or [] if isinstance(k, str) and re.fullmatch(r"[A-Z]", k)})
    answers = dict(a["answers"])
    if keys:
        answers[qid] = keys
    else:
        answers.pop(qid, None)
    pos = a["position"] if position is None else max(0, min(int(position), len(a["qids"]) - 1))
    _q("UPDATE exam_attempts SET answers = %s, position = %s WHERE id = %s", (json.dumps(answers), pos, a["id"]),
       rows=False)
    return {"saved": qid, "answered": len(answers)}


def submit(attempt_id, member, team):
    """Mark the attempt (a question is right only with exactly the right keys) → the review."""
    a = _own(attempt_id, member)
    if a["finished_at"]:
        return review(attempt_id, member)
    e = _q("SELECT * FROM exams WHERE id = %s", (a["exam_id"],), one=True)
    qs = _questions(a["exam_id"], a["qids"])
    right, by_domain = 0, {}
    for q in qs:
        ok = sorted(a["answers"].get(q["qid"], [])) == sorted(q["answer"])
        right += ok
        d = (q["domain"] or "").split(".")[0] or "-"
        by_domain.setdefault(d, [0, 0])
        by_domain[d][0] += ok
        by_domain[d][1] += 1
    total = len(qs) or 1
    score = round(100 * right / total)
    _q("UPDATE exam_attempts SET finished_at = now(), correct = %s, total = %s, score = %s, passed = %s, "
       "by_domain = %s WHERE id = %s",
       (right, len(qs), score, score >= e["pass_percent"], json.dumps(by_domain), a["id"]), rows=False)
    return review(attempt_id, member)


def review(attempt_id, member):
    """A submitted attempt: score, pass/fail, per domain, every question with the member's
    answer, the right one and the explanation."""
    a = _own(attempt_id, member)
    if not a["finished_at"]:
        raise ExamError("submit the attempt first")
    e = _q("SELECT * FROM exams WHERE id = %s", (a["exam_id"],), one=True)
    names = {d["id"]: d["name"] for d in (e["domains"] or [])}
    return {"id": a["id"], "done": True,
            "exam": {"id": e["id"], "name": e["name"], "pass_percent": e["pass_percent"]},
            "score": a["score"], "passed": a["passed"], "correct": a["correct"], "total": a["total"],
            "finished_at": a["finished_at"].isoformat(),
            "minutes": max(1, round((a["finished_at"] - a["started_at"]).total_seconds() / 60)),
            "by_domain": [{"domain": d, "name": names.get(d, "Unassigned" if d == "-" else f"Domain {d}"),
                           "correct": v[0], "total": v[1]} for d, v in sorted((a["by_domain"] or {}).items())],
            "questions": [{"qid": q["qid"], "question": q["question"], "options": q["options"],
                           "answer": q["answer"], "picked": a["answers"].get(q["qid"], []),
                           "right": sorted(a["answers"].get(q["qid"], [])) == sorted(q["answer"]),
                           "explanation": q["explanation"], "refs": q["refs"]}
                          for q in _questions(a["exam_id"], a["qids"])]}


def forget_member(member):
    """A member is deleted: their attempts go too."""
    return _q("DELETE FROM exam_attempts WHERE member = %s", (member,), rows=False)
