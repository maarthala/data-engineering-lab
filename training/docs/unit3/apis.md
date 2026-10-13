# Consuming APIs with `requests`

## Concept
Not all data lives in your database. **APIs** (web services you call over HTTP) are a major
source: exchange rates, weather, payment providers, marketing platforms, shipping trackers.
Pulling from an API is a classic **ingest** step ([1.1](../unit1/what-is-de.md)). In Python the
standard tool is the **`requests`** library.

A real, ShopFlow-shaped need: orders arrive in **different currencies** (`USD`, `GBP`, `EUR`,
`INR`). To report total revenue you need **FX rates** — which come from an API. We'll use the
free, no-key [Frankfurter](https://frankfurter.app) API.

### How an HTTP API call works

Calling an API is a **conversation** with two turns. You send a **request**; the server sends
back a **response**. That's the whole model — everything below is just the details of each half.

**Your request** is made of:

- A **method** — the *verb*. **`GET`** means "read / fetch me this" (the common one for
  ingestion); **`POST`** means "here's some data, create/do something with it".
- A **URL** — *which* resource, e.g. `https://api.frankfurter.app/latest`.
- **Params** — key/value pairs tacked onto the URL as a **query string**
  (`?from=USD&to=GBP`) that *filter or shape* what you get back.
- **Headers** — metadata *about* the request: who you are (an API key/token), what format you'll
  accept. They ride *alongside* the URL, not in it.

**The server's response** is made of:

- A **status code** — a 3-digit number saying how it went. **`200`** = OK, **`404`** = not
  found, **`429`** = you're being rate-limited (slow down), **`5xx`** = the server broke. As a
  rule: `2xx` good, `4xx` you got something wrong, `5xx` they did.
- A **body** — the actual payload, almost always **JSON** (a text format of nested
  keys/values that maps cleanly onto Python dicts and lists).

Tie it back to the pipeline: **an API is a *source*** ([1.1](../unit1/what-is-de.md)). You send a
request, parse the JSON response, and land the rows as-is in **Bronze** — exactly like reading a
file or a database table, just over the network.

In Python, the **`requests`** library handles both halves for you. We import it once:

```python
import requests
```

!!! note "This lab needs outbound internet"
    Unlike the rest of the course (which runs against the local stack), calling a public API
    needs the notebook to reach the internet. Everything else here — Postgres, Trino, Spark — is
    local.

## Lab

### A basic GET request
This is the whole request/response round-trip in one cell: ask Frankfurter for the latest USD
rates against three currencies, then inspect what came back.

```python
resp = requests.get("https://api.frankfurter.app/latest",
                    params={"from": "USD", "to": "GBP,EUR,INR"},
                    timeout=10)

resp.status_code        # 200 means OK
resp.url                # see how params became a query string
data = resp.json()      # parse the JSON body into a Python dict
data
# {'amount':1.0,'base':'USD','date':'...','rates':{'GBP':0.78,'EUR':0.91,'INR':83.2}}
```

**Read it step by step:**

- **`requests.get(url, …)`** — sends a `GET` request to that URL and *waits* for the response.
  When it returns, `resp` is a **response object** holding everything the server sent back.
- **`params={"from": "USD", "to": "GBP,EUR,INR"}`** — `requests` turns this dict into the query
  string for you: it appends `?from=USD&to=GBP,EUR,INR` to the URL (and safely escapes any special
  characters). You give it a Python dict; it handles the URL plumbing.
- **`timeout=10`** — give up after 10 seconds instead of hanging forever. **Always** set a timeout
  on a network call; without one, a stuck server can freeze your whole pipeline.
- **`resp.status_code`** — the status number described above. `200` here means the server
  understood and answered.
- **`resp.url`** — the *final* URL that was actually called. Handy for confirming your `params`
  landed as the query string you expected.
- **`resp.json()`** — parses the JSON **text** in the body into a real Python **dict** (nested
  dicts/lists) you can index and loop over. (Its sibling **`resp.text`** gives you the *raw,
  unparsed string* — reach for that only when the body isn't JSON.)

**The result** (`data`) is a dict: `amount` and `base` echo your request, `date` is when the rates
are from, and **`rates`** is the nested dict you actually want — `{'GBP': 0.78, 'EUR': 0.91, …}`.

### Always check for errors
Networks and services fail — handle it instead of trusting the response:

```python
def get_rates(base="USD", symbols="GBP,EUR,INR"):
    try:
        r = requests.get("https://api.frankfurter.app/latest",
                         params={"from": base, "to": symbols}, timeout=10)
        r.raise_for_status()             # raise if status is 4xx/5xx
        return r.json()["rates"]
    except requests.RequestException as e:
        print("API call failed:", e)
        return {}

rates = get_rates()
print(rates)
```

**Read it step by step:**

- **`def get_rates(base="USD", symbols="GBP,EUR,INR")`** — wrap the call in a function so you can
  reuse it. The arguments have **defaults**, so `get_rates()` with no arguments still works.
- **`try: … except requests.RequestException as e:`** — anything that can go wrong with the call
  (no network, DNS failure, timeout, a bad status once we raise on it) surfaces as a
  `requests.RequestException`. Catching it means one flaky call won't crash the whole notebook.
- **`r.raise_for_status()`** — the key safety line. `requests` does *not* raise on a `404` or
  `500` by itself — you'd get a response object with an error body. `raise_for_status()` turns any
  `4xx`/`5xx` status into an exception, so the `except` block catches it. Without this, a "Not
  Found" page could sail through as if it were real data.
- **`return r.json()["rates"]`** — on success, parse the JSON and pull out just the `rates` dict.
- **`except … print(...); return {}`** — on *any* failure, log the reason and return an **empty
  dict** so the caller gets a predictable, safe value instead of an explosion.

**The result:** `rates` is either `{'GBP': 0.78, 'EUR': 0.91, 'INR': 83.2}` on success, or `{}` if
the call failed — and either way your program keeps running.

### Headers & authentication (the shape you'll meet often)
Most real APIs need a key in a header. The pattern (Frankfurter needs none, so this is illustrative):

```python
headers = {"Authorization": "Bearer <token>", "Accept": "application/json"}
# requests.get(url, headers=headers, params=..., timeout=10)
```

**Read it step by step:**

- **`headers={...}`** — a dict of metadata sent *with* the request. Unlike `params`, headers don't
  appear in the URL.
- **`"Authorization": "Bearer <token>"`** — the most common auth pattern. An **API key** or
  **bearer token** is a secret string that identifies *you* to the API; the server checks it and
  either serves you (`200`) or rejects you (`401 Unauthorized` / `403 Forbidden`). "Bearer" just
  means "whoever bears this token is authorized".
- **`"Accept": "application/json"`** — tells the server *which format* you want back (JSON here).

!!! warning "Never hard-code secrets"
    A token in your code gets committed to git and shared with everyone who reads the notebook.
    Read it from an **environment variable** or a secrets store instead —
    `os.environ["API_TOKEN"]` — and keep the actual value out of the repo. In the lab, save it
    on **🔑 My variables** (as a *secret*) and every notebook has it in `os.environ`; see
    [My variables](../setup/workspace.md#my-variables-keys-and-settings-for-your-notebooks). On the
    cloud platforms this is Key Vault / Databricks secrets / Snowflake secrets.

### Load the response into pandas
API JSON → DataFrame is the everyday move — now it's tabular and joinable:

```python
import pandas as pd

rates = get_rates()
fx = (pd.DataFrame(rates.items(), columns=["currency", "rate_per_usd"])
        .assign(base="USD"))
fx
```

**Read it step by step:**

- **`rates.items()`** — a dict isn't tabular; `.items()` turns `{'GBP': 0.78, …}` into
  `[('GBP', 0.78), …]`, a list of (key, value) pairs — one pair per future row.
- **`pd.DataFrame(..., columns=["currency", "rate_per_usd"])`** — build a two-column table from
  those pairs and name the columns.
- **`.assign(base="USD")`** — add a constant `base` column recording what the rates are relative
  to. `.assign()` returns a *new* DataFrame with the extra column.

**The result** (`fx`) is a tidy table — one row per currency, with its rate — ready to **join**
against orders, exactly like any other source table.

### Put it together — convert ShopFlow revenue to USD
Combine the API rates with an orders DataFrame (the kind of enrichment you'll do for real):

```python
orders = pd.DataFrame({
    "order_id": [1, 2, 3, 4],
    "currency": ["USD", "GBP", "EUR", "INR"],
    "amount":   [120.0, 90.0, 100.0, 5000.0],
})
fx_all = pd.concat([fx, pd.DataFrame([{"currency": "USD", "rate_per_usd": 1.0, "base": "USD"}])])

usd = orders.merge(fx_all, on="currency", how="left")
usd["amount_usd"] = (usd["amount"] / usd["rate_per_usd"]).round(2)
usd[["order_id", "currency", "amount", "amount_usd"]]
```

**Read it step by step:**

- **`orders = pd.DataFrame({...})`** — a small stand-in orders table, one row per order with its
  currency and amount.
- **`fx_all = pd.concat([fx, pd.DataFrame([{...USD, 1.0...}])])`** — the API gave rates for *other*
  currencies relative to USD, but not USD→USD. We `concat` (stack on) a row that says
  `USD = 1.0` so USD orders have a rate to divide by too.
- **`orders.merge(fx_all, on="currency", how="left")`** — a **left join** (same idea as
  [2.2](../unit2/joins-aggregations.md)): match each order to its currency's rate, keeping *every*
  order even if its currency is missing.
- **`usd["amount"] / usd["rate_per_usd"]`** — the rates are "how many of this currency per 1 USD",
  so *dividing* the local amount by the rate gives USD. `.round(2)` trims to cents.

**The result** is the orders table with a new **`amount_usd`** column — every order now comparable
in one currency, which is the point of pulling the FX API in the first place.

### Pagination (when results come in pages)
Big APIs return data in pages; loop until there's no "next":

```python
# Pattern (pseudocode-ish): keep fetching until the API says there's no more
all_rows, page = [], 1
while True:
    r = requests.get("https://example.com/api/orders",
                     params={"page": page, "per_page": 100}, timeout=10)
    batch = r.json().get("data", [])
    if not batch:
        break
    all_rows.extend(batch)
    page += 1
```

An API rarely hands you a million rows in one response — it splits them into **pages** and makes
you ask for each. So ingestion means **looping**: fetch a page, keep the rows, ask for the next,
and stop when there are none left.

**Read it step by step:**

- **`all_rows, page = [], 1`** — an accumulator for everything we collect, and a page counter
  starting at page 1.
- **`while True:`** — loop indefinitely; we'll `break` out ourselves once the data runs out.
- **`params={"page": page, "per_page": 100}`** — ask for *this* page, 100 rows at a time. (The
  exact param names vary by API — check its docs; some use `offset`/`limit` or a `cursor` token.)
- **`batch = r.json().get("data", [])`** — pull this page's rows out of the JSON. `.get("data",
  [])` returns `[]` if the key is absent, so an empty/last page is handled safely.
- **`if not batch: break`** — an empty batch means "no more data" — stop looping. This is the
  crucial exit condition; without it a `while True` never ends.
- **`all_rows.extend(batch)`** — append this page's rows to the running list.
- **`page += 1`** — advance to the next page and go round again.

**The result:** after the loop, `all_rows` holds *every* row across *all* pages — ready to hand to
`pd.DataFrame(all_rows)` and land as Bronze.

!!! tip "Rate limits & retries"
    Loop hard enough and an API may answer **`429 Too Many Requests`** — its way of saying "slow
    down". The polite response is to **wait and retry**: sleep briefly (often the response's
    `Retry-After` header tells you how long), then try that page again, backing off longer each
    time it keeps failing. The same retry idea covers transient **`5xx`** blips. Don't hammer a
    struggling server — pause and re-attempt.

## Challenge
Write `to_usd(orders_df)` that fetches live rates with `get_rates()`, joins them to an orders
DataFrame, and returns it with an `amount_usd` column — defaulting the rate to `1.0` for any
currency the API didn't return (so a missing rate never drops a row).

??? note "Solution"
    ```python
    def to_usd(orders_df):
        rates = get_rates()
        rates["USD"] = 1.0
        fx = pd.DataFrame(rates.items(), columns=["currency", "rate_per_usd"])
        out = orders_df.merge(fx, on="currency", how="left")
        out["rate_per_usd"] = out["rate_per_usd"].fillna(1.0)   # never drop a row
        out["amount_usd"] = (out["amount"] / out["rate_per_usd"]).round(2)
        return out

    to_usd(orders)
    ```

    **Read it step by step:**

    - **`rates = get_rates()`** — reuse the error-handled fetcher; you get the live `rates` dict
      (or `{}` if the call failed).
    - **`rates["USD"] = 1.0`** — add the USD→USD rate the API omits, so USD rows convert cleanly.
    - **`pd.DataFrame(rates.items(), …)`** — reshape the dict into the two-column `fx` table.
    - **`orders_df.merge(fx, on="currency", how="left")`** — left-join so *no order is dropped*
      even if its currency has no rate.
    - **`out["rate_per_usd"].fillna(1.0)`** — a `left` join leaves `NaN` where the rate was
      missing; `fillna(1.0)` replaces those with 1.0 so the division still works and the row
      survives (the whole point of the challenge).
    - **`out["amount"] / out["rate_per_usd"]`** — convert to USD and round to cents.

!!! tip "🎯 The same pattern on Azure, Databricks, Snowflake & Fabric"
    **What you just did:** ingested data from a REST API and shaped it for joining.

    - **Azure Data Factory** — the **REST / HTTP connector** (Copy activity) pulls APIs no-code,
      with built-in pagination rules.
    - **Azure Databricks / Fabric** — call `requests` in a notebook exactly like this, or use a
      connector; land the result as a Bronze table.
    - **Snowflake** — **external access integrations** let Snowpark Python call APIs; or stage the
      pulled files and `COPY INTO`.

    "Pull from an API, handle errors, page through results, land it as a table" is universal.

## Key terms, at a glance
| Term | Plain meaning |
|---|---|
| **HTTP request** | What you send: a method (`GET`/`POST`) + URL + optional params & headers |
| **HTTP response** | What comes back: a status code + a body (usually JSON) |
| **`requests.get(url, params=…)`** | Make an HTTP GET; `params` become the `?key=value` query string |
| **Status code** | Result number: `200` OK, `404` not found, `429` rate-limited, `5xx` server error |
| **`raise_for_status()`** | Turn a `4xx`/`5xx` status into an exception (requests won't by default) |
| **JSON** | Text format of nested keys/values that maps onto Python dicts/lists |
| **`.json()` vs `.text`** | Parse the body into a dict/list *vs.* get the raw unparsed string |
| **Params** | Key/values appended to the URL as a query string — filter/shape the result |
| **Headers** | Metadata sent alongside the request (auth token, `Accept` format) |
| **Bearer token / API key** | Secret that authenticates you — read from a secret, never hard-code |
| **Pagination** | Loop pages (`page`/`per_page`) until the API returns no more |
| **Rate limit (`429`)** | "Slow down" — wait (`Retry-After`) and retry with backoff |
| **`timeout`** | Cap how long a call may hang — always set it |

## You can now…
- Call a REST API with `requests`, pass params, and parse JSON
- Handle failures with `raise_for_status` and `try/except`
- Load an API response into pandas and join it to your data
- Recognise the pagination pattern for large result sets
