"""Course numbering — every unit and lesson number comes from the nav order in mkdocs.yml.

Insert a unit or a lesson anywhere in the nav and everything after it renumbers itself:
the nav, page headings and references. Nothing in the pages carries a hard-coded number.

  nav            a section is a unit (0, 1, 2 …), a page in it a lesson (4.1, 4.2 …).
                 A label starting with "+ " is a sub-lesson of the lesson above it (4.2.1).
                 Top-level pages (Home, Course overview, Help) get no number.
  page heading   the first "# Title" of a lesson becomes "# 4.2 Title".
  links          [4.3](../unit4/transform-silver.md), [Unit 4](../unit4/fundamentals.md):
                 the number in the link text is replaced by the target page's number.
  tokens         for places without a link (plain text, headings, diagrams, code comments),
                 paths relative to docs/:
                   {lesson:unit4/read-bronze.md}   → 4.2
                   {unit:unit4/read-bronze.md}     → 4
                   {count:unit4/read-bronze.md}    → lessons in that unit (sub-lessons not counted)

A plain "Unit 4" / "lesson 4.2" outside a link or token would go stale on the next insert, so
it is reported as a warning — and `mkdocs build --strict` (make docs, the CI) fails on it.
"""
import logging
import posixpath
import re

log = logging.getLogger("mkdocs.hooks.numbering")

SUB = "+ "
_num = {}      # page path → "4.2" / "4.2.1"
_unit = {}     # page path → "4"
_count = {}    # page path → lessons in its unit

TOKEN = re.compile(r"\{(lesson|unit|count):([^}\s]+)\}")
LINK = re.compile(r"(?<!!)\[([^\[\]]*)\]\(([^)\s]+)((?:\s+\"[^\"]*\")?)\)")
UNIT_IN_TEXT = re.compile(r"\b([Uu]nits?)(\s+)(\d+)(?![\d.])")
LESSON_IN_TEXT = re.compile(r"(?<![\w.])(\d+\.\d+(?:\.\d+)?)(?![\w.])")
HEADING_NUMBER = re.compile(r"^#\s+\d+(?:\.\d+)*\s")
STALE = re.compile(r"\b(?:[Uu]nits?|[Ll]essons?)\s+\d+(?:\.\d+)*")
FENCE = re.compile(r"^(```|~~~)")


def on_config(config):
    """Number the nav: units in order from 0, lessons in order within each unit."""
    _num.clear(), _unit.clear(), _count.clear()
    numbered, unit = [], -1
    for entry in config.get("nav") or []:
        if not isinstance(entry, dict):
            numbered.append(entry)
            continue
        (label, value), = entry.items()
        if not isinstance(value, list):                 # top-level page, no number
            numbered.append(entry)
            continue
        unit += 1
        lessons, lesson, sub, paths = [], 0, 0, []
        for child in value:
            if not isinstance(child, dict):
                log.warning("numbering: give '%s' a nav label (unit %s)", child, unit)
                lessons.append(child)
                continue
            (title, path), = child.items()
            if title.startswith(SUB):
                if not lesson:
                    log.warning("numbering: sub-lesson '%s' has no lesson above it", title)
                sub += 1
                number, title = f"{unit}.{lesson}.{sub}", title[len(SUB):]
            else:
                lesson, sub = lesson + 1, 0
                number = f"{unit}.{lesson}"
            _num[path], _unit[path] = number, str(unit)
            paths.append(path)
            lessons.append({f"{number} {title}": path})
        for path in paths:
            _count[path] = str(lesson)
        numbered.append({f"{unit} · {label}": lessons})
    config["nav"] = numbered
    return config


def _target(page_path, href):
    """A link's href → the docs path it points to (None for external links / anchors only)."""
    href = href.split("#", 1)[0]
    if not href or re.match(r"^[a-z][a-z0-9+.-]*:", href) or href.startswith("/"):
        return None
    return posixpath.normpath(posixpath.join(posixpath.dirname(page_path), href))


def _token(match, page_path):
    kind, path = match.groups()
    table = {"lesson": _num, "unit": _unit, "count": _count}[kind]
    if path not in table:
        log.warning("numbering: %s — {%s:%s} is not a page in the nav", page_path, kind, path)
        return match.group(0)
    return table[path]


def _link(match, page_path):
    text, href, title = match.groups()
    path = _target(page_path, href)
    if path in _num:
        if UNIT_IN_TEXT.search(text):
            text = UNIT_IN_TEXT.sub(lambda m: m.group(1) + m.group(2) + _unit[path], text, count=1)
        else:
            text = LESSON_IN_TEXT.sub(_num[path], text, count=1)
    return f"[{text}]({href}{title})"


def _lint(markdown, page_path):
    """Warn about hard-coded "Unit 4" / "lesson 4.2" — they go stale on the next insert."""
    for i, line in enumerate(markdown.splitlines(), 1):
        bare = LINK.sub("", TOKEN.sub("", line))
        for m in STALE.finditer(bare):
            log.warning("numbering: %s:%s hard-coded '%s' — use a link or a {lesson:…}/{unit:…} token",
                        page_path, i, m.group(0))


def on_page_markdown(markdown, page, config, files):
    path = page.file.src_uri
    _lint(markdown, path)
    markdown = TOKEN.sub(lambda m: _token(m, path), markdown)
    markdown = LINK.sub(lambda m: _link(m, path), markdown)
    if path in _num:                                    # "# Title" → "# 4.2 Title" (first H1 only)
        lines, fenced = markdown.split("\n"), False
        for i, line in enumerate(lines):
            if FENCE.match(line):
                fenced = not fenced
            elif not fenced and line.startswith("# "):
                if HEADING_NUMBER.match(line):
                    log.warning("numbering: %s heading '%s' has a hard-coded number", path, line)
                lines[i] = f"# {_num[path]} {line[2:]}"
                break
        markdown = "\n".join(lines)
    return markdown
