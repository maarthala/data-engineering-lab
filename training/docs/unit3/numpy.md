# NumPy — fast numeric arrays

## Concept
**NumPy** is the numerical foundation Python data work is built on — **pandas itself stores its
columns as NumPy arrays**, and so do scikit-learn, PyArrow, and most of the ecosystem. Its core
object is the **`ndarray`**: a typed, fixed-size array that supports **vectorised** operations —
math applied to the whole array at once, in fast C code, with no Python loop.

You won't write huge amounts of NumPy as a data engineer, but you need to *read* it and reach for
it for numeric transforms, and understanding it explains *why* pandas/Spark are fast.

### Why NumPy — vectorisation
A plain Python list can hold anything — a number, a string, another list — so Python must check the
*type* of every element on every operation. An **`ndarray`** is different: it is a **fixed-type**
array (all `float64`, or all `int64`, …) stored in one contiguous block of memory. Because the type
is known up front, NumPy can hand the whole array to a tight **C loop** and process it in bulk.

That bulk operation is called a **vectorised** operation: instead of writing a Python `for` loop
that touches one element at a time (slow — every step pays the Python interpreter tax), you write
`amounts * 1.2` and the multiply runs across all elements at once in optimised C. Same result,
often 10–100× faster, and far less code to read. This "operate on the whole column at once" idea is
exactly what makes pandas and Spark fast too — so learning it here pays off everywhere.

```python
import numpy as np
```

**Read it step by step:**

- **`import numpy as np`** — load the library and give it the conventional short name `np`. Every
  NumPy function you call will start with `np.` (e.g. `np.array`, `np.where`). Everyone in the data
  world uses this exact alias, so match it.

## Lab

### Arrays and vectorised math
The whole point: operate on an entire array at once — no `for` loop.

```python
amounts = np.array([120.0, 40.0, 300.0, 90.0, 55.0])

amounts * 1.2            # add 20% tax to every element at once
amounts.sum()            # 605.0
amounts.mean()           # 121.0
amounts.max(), amounts.min()
amounts.dtype            # float64  (arrays are typed)
amounts.shape            # (5,)
```

**Read it step by step:**

- **`np.array([...])`** — the most common way to *create* an array: hand it a Python list and NumPy
  builds an `ndarray` from it. Because every value here has a decimal point, NumPy picks the element
  type `float64` automatically. The result is a 1-D array of 5 numbers.
- **`amounts * 1.2`** — a **vectorised** multiply: every element is multiplied by `1.2` at once (no
  loop), returning a *new* array of the same shape. This is the "add 20% tax to the whole column"
  move — the core NumPy idiom.
- **`amounts.sum()`** / **`amounts.mean()`** — **aggregations**: they collapse the whole array down
  to a single number (the total, `605.0`; the average, `121.0`).
- **`amounts.max(), amounts.min()`** — the largest and smallest element. The trailing comma makes
  this a tuple, so you get both back at once: `(300.0, 40.0)`.
- **`amounts.dtype`** — reports the element type: **`float64`** (a 64-bit floating-point number).
  Every element shares this one type — that fixed type is what lets NumPy be fast.
- **`amounts.shape`** — reports the array's dimensions as a tuple. **`(5,)`** means "1-D, 5
  elements long". (A 2-D array would show `(rows, cols)` — you'll see that below.)

Compare to plain Python — same result, but NumPy does it in one vectorised step instead of a loop:

```python
# plain Python (a loop)      vs      NumPy (vectorised)
taxed_loop = [a * 1.2 for a in amounts]
taxed_np   = amounts * 1.2
```

**Read it step by step:**

- **`taxed_loop`** — the plain-Python way: a list comprehension that walks the array element by
  element, multiplying each one in the (slow) Python interpreter. It works, but every step pays the
  per-element Python cost.
- **`taxed_np`** — the NumPy way: one vectorised expression that does the whole multiply in C. Same
  numbers come out, but there's no visible loop and it runs far faster on large arrays.

!!! note "Why the vectorised version wins"
    For 5 numbers you'd never notice. For 5 *million* the loop pays the Python interpreter cost a
    million times, while `amounts * 1.2` pays it once and lets C do the rest. That gap is the whole
    reason columnar engines exist — and why the rule of thumb in NumPy (and pandas, and Spark) is
    **"if you're writing a `for` loop over rows, look for the vectorised version first."**

### Boolean masks & `where`
Vectorised filtering and conditionals — the pattern pandas uses under the hood:

```python
mask = amounts > 100            # array([ True, False,  True, False, False])
amounts[mask]                   # array([120., 300.]) — keep matching elements

# np.where(cond, if_true, if_false) — vectorised CASE WHEN
band = np.where(amounts >= 100, "large", "small")
```

**Read it step by step:**

- **`amounts > 100`** — a *comparison* is also vectorised: it tests every element against `100` and
  returns a **boolean mask**, an array of `True`/`False` the same length as `amounts`. Here `120`
  and `300` are over `100`, so the mask is `[True, False, True, False, False]`.
- **`amounts[mask]`** — **boolean masking** (indexing an array with a boolean array). NumPy keeps
  only the elements where the mask is `True`, giving `[120., 300.]`. This is exactly how a pandas
  `df[df.amount > 100]` filter works under the hood — no loop, just a mask.
- **`np.where(cond, if_true, if_false)`** — a vectorised **if/else**, the NumPy equivalent of SQL's
  `CASE WHEN`. It walks the condition array and, element by element, picks the `if_true` value where
  the condition holds and the `if_false` value where it doesn't. Here every element ≥ 100 becomes
  `"large"`, the rest `"small"`, so `band` is `["large","small","large","small","small"]`.

### Aggregations & simple stats
```python
np.round(amounts, 2)
np.percentile(amounts, 50)      # median
np.cumsum(amounts)              # running total (like SQL SUM() OVER)
```

**Read it step by step:**

- **`np.round(amounts, 2)`** — round every element to 2 decimal places, vectorised. Returns a new
  array the same shape as `amounts`; the original is untouched.
- **`np.percentile(amounts, 50)`** — the value below which 50% of the data falls, i.e. the
  **median**. (`np.percentile(..., 90)` would give the 90th percentile.) This collapses the array to
  a single number.
- **`np.cumsum(amounts)`** — the **cumulative** (running) sum: element *i* of the result is the sum
  of everything up to and including position *i*, so `[120, 40, 300, …]` becomes
  `[120, 160, 460, …]`. Unlike `sum()`, this returns an array the *same length* as the input — it's
  the NumPy version of SQL's `SUM() OVER (...)` window.

### 2-D arrays (a matrix) & axes
So far every array has been 1-D. NumPy arrays can have more dimensions — a 2-D array is just a grid
(rows × columns), like a spreadsheet. When you aggregate a 2-D array you have to say *which
direction* to collapse, and that's what the **`axis`** argument is for.

```python
grid = np.array([[1, 2, 3],
                 [4, 5, 6]])
grid.shape          # (2, 3)
grid.sum(axis=0)    # column sums → [5, 7, 9]
grid.sum(axis=1)    # row sums    → [6, 15]
```

**Read it step by step:**

- **`np.array([[...], [...]])`** — passing a *list of lists* builds a 2-D array: two rows, three
  columns.
- **`grid.shape`** — now reports **`(2, 3)`** — `(rows, columns)`. The first number is `axis=0`, the
  second is `axis=1`.
- **`grid.sum(axis=0)`** — sum *down* axis 0 (across the rows), collapsing each **column** to one
  number: `[1+4, 2+5, 3+6] = [5, 7, 9]`. The axis you name is the one that disappears.
- **`grid.sum(axis=1)`** — sum *along* axis 1 (across the columns), collapsing each **row** to one
  number: `[1+2+3, 4+5+6] = [6, 15]`.

!!! tip "How to remember `axis`"
    The `axis` you pass is the dimension that gets **eaten**. `axis=0` eats the rows, leaving one
    value per column (column sums). `axis=1` eats the columns, leaving one value per row (row sums).
    Leave `axis` off entirely and NumPy sums *everything* into a single scalar. This same `axis=`
    argument works on `mean`, `std`, `min`, `max`, and friends.

### Reproducible random data (handy for tests/samples)
```python
rng = np.random.default_rng(seed=42)   # seed → same numbers every run
rng.integers(1, 100, size=5)
```

**Read it step by step:**

- **`np.random.default_rng(seed=42)`** — create a random-number *generator*. The **seed** fixes its
  starting point, so the "random" numbers are the same on every run — essential for reproducible
  tests and examples. Change the seed (or drop it) and you get a different stream.
- **`rng.integers(1, 100, size=5)`** — draw 5 random integers from `1` up to (but not including)
  `100`, returned as a 1-D array of shape `(5,)`.

### NumPy ⇄ pandas
They're two sides of one coin — a pandas column *is* a NumPy array:

```python
import pandas as pd
s = pd.Series(amounts)
s.to_numpy()               # pandas → NumPy
pd.Series(np.arange(5))    # NumPy → pandas
```

**Read it step by step:**

- **`pd.Series(amounts)`** — wrap the NumPy array in a pandas `Series` (a labelled 1-D column). No
  copy of the numbers is needed — pandas stores its values *as* a NumPy array underneath.
- **`s.to_numpy()`** — go the other way: hand back the raw `ndarray` inside the Series. Reach for
  this when you want to drop into NumPy for a fast numeric transform.
- **`np.arange(5)`** — a quick array-*creation* helper: like Python's `range`, it produces
  `[0, 1, 2, 3, 4]` (start at 0, stop before 5). **`pd.Series(...)`** then wraps it as a column.

!!! info "This is why pandas *is* NumPy"
    Because a pandas column is physically a NumPy array, every vectorised trick above —
    element-wise math, boolean masks, `axis=` aggregations — reappears in pandas with almost the
    same syntax. Learn it once here and you've learned the engine behind the next lesson.

## Challenge
Given `amounts = np.array([120., 40., 300., 90., 55.])`, use NumPy (no loops) to compute: the
**share of total** each amount represents (as percentages, rounded to 1 decimal), and a label
array marking each as `"above"` or `"below"` the mean.

??? note "Solution"
    ```python
    amounts = np.array([120., 40., 300., 90., 55.])
    share = np.round(amounts / amounts.sum() * 100, 1)     # [19.8, 6.6, 49.6, 14.9, 9.1]
    label = np.where(amounts > amounts.mean(), "above", "below")
    print(share)
    print(label)
    ```

    **Read it step by step:**

    - **`amounts / amounts.sum()`** — this is **broadcasting** in action: you're dividing an array
      (shape `(5,)`) by a single scalar (`amounts.sum()`, one number). NumPy "stretches" the scalar
      to match every element, so each amount is divided by the total — no loop, one expression.
    - **`* 100`** — another broadcast: scale every share into a percentage.
    - **`np.round(..., 1)`** — round the whole array to 1 decimal place.
    - **`np.where(amounts > amounts.mean(), "above", "below")`** — a vectorised `CASE WHEN`: compare
      every element to the mean and label it `"above"` or `"below"` accordingly, with no loop.

    !!! note "Broadcasting, briefly"
        **Broadcasting** is how NumPy operates between arrays of *different shapes*. When one side
        is a single number (or a smaller-shaped array), NumPy virtually stretches it to fit the
        larger one instead of forcing you to build a same-shape array by hand. `amounts / total`
        and `amounts * 1.2` both rely on it — the scalar is broadcast across all 5 elements.

!!! tip "🎯 The same NumPy on Azure, Databricks, Snowflake & Fabric"
    NumPy is pure Python numerics — it runs identically in **Databricks**, **Fabric**, and
    **Synapse** notebooks and in **Snowpark** Python. At scale, the *vectorised* idea reappears:
    Spark and Snowflake execute column operations in bulk (like NumPy) instead of row-by-row —
    which is exactly why they're fast.

## Key terms, at a glance
| Term | Plain meaning |
|---|---|
| **`ndarray`** | A typed, fixed-size numeric array — NumPy's core object |
| **Vectorised op** | Math on a whole array at once, in fast C (no Python loop) |
| **dtype** | The element type shared by every value (`float64`, `int64`, …) |
| **shape** | The array's dimensions as a tuple — `(5,)` = 1-D, `(2, 3)` = 2-D |
| **`np.array` / `np.arange`** | Create an array from a list / from a numeric range |
| **Boolean mask** | A `True`/`False` array; `arr[mask]` keeps matching elements |
| **Broadcasting** | Auto-stretching a scalar/smaller array to operate across a larger one |
| **`np.where`** | Vectorised if/else (`CASE WHEN`) — pick per element by a condition |
| **Aggregation** | Collapse an array to a summary (`sum`/`mean`/`std`/`min`/`max`) |
| **axis** | Which dimension an aggregation eats (0 = rows→col sums, 1 = cols→row sums) |

## You can now…
- Create arrays and run vectorised math and aggregations (no loops)
- Filter with boolean masks and branch with `np.where`
- Explain why NumPy underlies pandas — and why vectorisation makes engines fast
