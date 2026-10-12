# Tutorial: JSONL to Postgres with Airflow

This walkthrough builds a small but complete data pipeline on the Airflow
install from [README.md](README.md). You'll end up with a DAG that:

1. validates a JSONL file of orders, refusing to continue if any line is bad
2. loads the rows into a Postgres table called `raw_sample_data`
3. rolls them up with SQL into a second table called `sample_data_totals`
4. checks that the totals agree with the raw data

```
validate_sample_data ─┐
                      ├─> load_raw_sample_data ─> transform_sample_data_totals ─> check_sample_data_totals
create_tables ────────┘
```

Everything is already in the project, so you can run it first and read
the explanation afterwards, or work through it top to bottom.

| File | What it is |
| --- | --- |
| `data/sample_data.jsonl` | 30 valid order records |
| `data/sample_data_invalid.jsonl` | 6 records, 4 of them broken on purpose |
| `airflow_home/dags/sample_data_pipeline.py` | the DAG |
| `airflow_home/dags/sql/create_tables.sql` | `CREATE TABLE IF NOT EXISTS` for both tables |
| `airflow_home/dags/sql/transform_sample_data_totals.sql` | the aggregation query |
| `airflow_home/dags/sql/check_sample_data_totals.sql` | assertions the check task runs |

## Prerequisites

If you cloned this repo and ran `scripts/setup.sh` as the README
describes, steps 1 to 3 below are already done: Postgres has the role and
database, the provider is installed, and the Airflow connection exists.
Read them anyway so you know what the script did, then pick up at step 4.

Every command below assumes a terminal with the environment loaded:

```bash
cd ~/projects/airflow-learning
source .env && source .venv/bin/activate
```

`.env` sets `AIRFLOW_HOME`, puts `psql` on your `PATH`, and exports the
`PG*` variables so `psql` connects to the tutorial database without flags.

## Step 1: Postgres

Install and start Postgres with Homebrew if you don't already have it:

```bash
brew install postgresql@18
brew services start postgresql@18
```

Homebrew's `postgresql@18` is keg-only, so `psql` is not on your `PATH`
until you add it. `.env` does this for you.

Confirm the server is up:

```bash
pg_isready
```

Create a dedicated role and database for this project. Keeping Airflow's
learning data in its own database means you can drop it later without
touching anything else. Role and database creation need a superuser, which
on a Homebrew install is your macOS user:

```bash
psql -U "$(whoami)" -d postgres -c "CREATE ROLE airflow_learning WITH LOGIN PASSWORD 'airflow_learning';"
psql -U "$(whoami)" -d postgres -c "CREATE DATABASE airflow_learning OWNER airflow_learning;"
```

If either command says the object already exists, that's fine. Check you
can log in as the new role (the `PG*` variables in `.env` supply the host,
user, database and password):

```bash
psql -c "select current_user;"
```

The password here is deliberately weak because this database only ever
listens on localhost. Never reuse this pattern on a real server.

## Step 2: Install the Postgres provider

Airflow talks to databases through *provider* packages. The Postgres one
brings in `psycopg2` and a hook and registers the `postgres` connection
type. It is already in `pyproject.toml` as the `[postgres]` extra, so
`uv sync` installed it. If you were adding it fresh:

```bash
uv add "apache-airflow[postgres]==3.3.1" --constraint constraints.txt
```

Confirm it imports:

```bash
python -c "from airflow.providers.postgres.hooks.postgres import PostgresHook; print('ok')"
```

## Step 3: Tell Airflow about the database

Airflow keeps database credentials in *connections*, stored encrypted in
its own metadata database and looked up by id. The DAG refers to the
connection by id only, so the same code runs unchanged against a different
host in staging or production.

Because connections live in the metadata database, which is not in git,
each clone has to register its own. `scripts/setup.sh` does this; by hand
it is:

```bash
airflow connections add postgres_airflow_learning \
  --conn-type postgres \
  --conn-host localhost \
  --conn-port 5432 \
  --conn-login airflow_learning \
  --conn-password airflow_learning \
  --conn-schema airflow_learning
```

In Airflow's Postgres connection, `schema` means the database name, not a
Postgres schema. This trips everyone up once.

Check it:

```bash
airflow connections get postgres_airflow_learning
```

You can also create connections in the UI under Admin, Connections, or via
an environment variable named `AIRFLOW_CONN_POSTGRES_AIRFLOW_LEARNING`. The
CLI is the easiest to put in a setup script, which is exactly what
`scripts/setup.sh` does.

## Step 4: The data

Each line of `data/sample_data.jsonl` is one JSON object describing an
order:

```json
{"order_id": "ORD-1006", "customer_id": "CUST-007", "order_date": "2026-09-02", "region": "north", "category": "services", "product": "training", "quantity": 1, "unit_price": "750.00"}
```

| Field | Type | Rule |
| --- | --- | --- |
| `order_id` | string | unique within the file |
| `customer_id` | string | non-empty |
| `order_date` | `YYYY-MM-DD` | must be a real date |
| `region` | string | one of `north`, `south`, `west` |
| `category` | string | one of `hardware`, `software`, `services` |
| `product` | string | non-empty |
| `quantity` | integer | greater than 0 |
| `unit_price` | decimal string | zero or more, 2 decimal places |

`unit_price` is a string on purpose. JSON numbers are floats, and floats
cannot represent `0.10` exactly. Sending money through a string and parsing
it as `Decimal` avoids rounding surprises in the totals.

`data/sample_data_invalid.jsonl` has the same shape but four of its six
lines break a rule. You'll use it in step 7.

## Step 5: The DAG, piece by piece

Open `airflow_home/dags/sample_data_pipeline.py` and follow along.

### Configuration at the top

```python
POSTGRES_CONN_ID = "postgres_airflow_learning"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SOURCE_FILE = PROJECT_ROOT / "data" / "sample_data.jsonl"
```

Constants live together at the top so there is one place to change them.
The project root is worked out from the DAG file's own location instead
of being hard-coded, which keeps the file portable.

### The schema

```python
class SampleRecord(BaseModel):
    order_id: str = Field(min_length=1)
    order_date: date
    region: Literal["north", "south", "west"]
    quantity: int = Field(gt=0)
    unit_price: Decimal = Field(ge=0, decimal_places=2)
    ...
```

A pydantic model is the single definition of what a good row looks like.
It converts `"2026-09-02"` to a `date` and `"750.00"` to a `Decimal`, and
it rejects anything that does not fit with a message naming the field.
Both the validate task and the load task use it, so they can never
disagree about what counts as valid. pydantic is already installed because
Airflow itself depends on it.

### Task 1: `validate_sample_data`

A `@task`-decorated Python function. It reads the file line by line,
collects every problem (up to 20), checks for duplicate `order_id`s, and
raises a single `ValueError` listing all of them. Nothing touches Postgres.
If this task fails, nothing downstream runs.

It returns a small dict with the file path and row count. Airflow passes
that to the next task through XCom. Passing a summary rather than the rows
themselves is the right habit: XCom lives in Airflow's metadata database
and is for coordination, not for moving data.

The `params` argument is how the task reads the `source_file` parameter,
which defaults to `data/sample_data.jsonl` but can be overridden per run.

### Task 2: `create_tables`

```python
create_tables = SQLExecuteQueryOperator(
    task_id="create_tables",
    conn_id=POSTGRES_CONN_ID,
    sql="sql/create_tables.sql",
)
```

`SQLExecuteQueryOperator` runs SQL against any connection. A path ending in
`.sql` is read from disk relative to the DAG file, so the SQL lives in its
own file with syntax highlighting instead of inside a Python string.

The DDL uses `CREATE TABLE IF NOT EXISTS` so it is safe to run every time.
`raw_sample_data` keeps two extra columns, `source_file` and `loaded_at`,
so you can always tell where a row came from. Its primary key is
`(order_id, source_file)`, which allows the same order to appear in two
different files without a collision.

This task has no dependency on validation, so Airflow runs the two in
parallel.

### Task 3: `load_raw_sample_data`

Another `@task`. It re-reads the file through the same pydantic model,
then, inside one transaction, deletes any rows from a previous load of
the same file and inserts the new ones. If the insert fails halfway, the
rollback leaves the old rows in place.

Delete-then-insert keyed on `source_file` is what makes the task
*idempotent*: running it twice gives the same result as running it once.
That matters because Airflow retries failed tasks and people re-run DAGs,
and you do not want duplicates every time.

It uses `PostgresHook` to get a plain `psycopg2` connection from the
Airflow connection id. The hook handles credentials; the code handles SQL.

### Task 4: `transform_sample_data_totals`

Another `SQLExecuteQueryOperator`, pointing at
`sql/transform_sample_data_totals.sql`:

```sql
DELETE FROM sample_data_totals;

INSERT INTO sample_data_totals (...)
SELECT region, category,
       COUNT(*), SUM(quantity), SUM(quantity * unit_price),
       MIN(order_date), MAX(order_date)
FROM raw_sample_data
GROUP BY region, category;
```

The operator runs the whole file in one transaction by default, so the
`DELETE` and the `INSERT` commit together and nobody ever sees an empty
totals table. A full rebuild is the simplest correct strategy for a small
aggregate. For a large one you'd switch to incremental upserts keyed on a
date column.

### Task 5: `check_sample_data_totals`

`SQLCheckOperator` runs a query and fails the task if any value in the
first row is false, null, zero or empty. `sql/check_sample_data_totals.sql`
returns three booleans: the totals table is not empty, its revenue sums to
the raw revenue, and its order counts sum to the raw row count. A data
check at the end of a pipeline is cheap insurance against a transform
that silently drops rows.

### Wiring

```python
summary = validate_sample_data()
loaded = load_raw_sample_data(summary)
create_tables >> loaded
loaded >> transform_sample_data_totals >> check_sample_data_totals
```

Calling a `@task` function and passing its result to another creates both
the XCom hand-off and the dependency. `>>` adds dependencies between
operators that don't exchange data.

## Step 6: Run it

The quickest way is from the command line, which runs the whole DAG in a
single process and prints the logs:

```bash
airflow dags test sample_data_pipeline
```

Look for these lines in the output:

```
sample_data.jsonl: 30 rows passed validation
replaced 0 row(s) with 30 row(s) from sample_data.jsonl
Record: (True, True, True)
DagRun Finished: ... state=success
```

Then look at the tables:

```bash
psql -c "select region, category, order_count, total_revenue from sample_data_totals order by 1, 2;"
```

```
 region | category | order_count | total_revenue
--------+----------+-------------+---------------
 north  | hardware |           4 |      10760.97
 north  | services |           5 |       7500.00
 north  | software |           3 |        785.00
 south  | hardware |           2 |       2697.00
 ...
```

To run it from the UI instead, start `airflow standalone`, open
http://localhost:8080, find `sample_data_pipeline`, and press the trigger
button. The trigger form shows the `source_file` parameter, which you can
leave at its default. Click into the run to watch the graph light up, and
click a task to read its log.

## Step 7: Break it

Point the DAG at the invalid file:

```bash
airflow dags test sample_data_pipeline --conf "{\"source_file\": \"$PWD/data/sample_data_invalid.jsonl\"}"
```

The validate task fails with every problem listed:

```
ValueError: sample_data_invalid.jsonl failed validation with 4 problem(s):
  line 2: quantity: Input should be greater than 0
  line 3: region: Field required
  line 4: order_date: Input should be a valid date or datetime, month value is outside expected range of 1-12
  line 5: category: Input should be 'hardware', 'software' or 'services'
```

The DAG run is marked failed, the load task never starts, and Postgres is
unchanged. Query the tables again to prove it. Collecting every error in
one pass, rather than stopping at the first, means whoever fixes the file
only has to make one trip.

## Step 8: Run it again

```bash
airflow dags test sample_data_pipeline
```

This time the load log says `replaced 30 row(s) with 30 row(s)`. The row
count in `raw_sample_data` is still 30, not 60, and the totals are
identical. That is idempotency doing its job.

## Naming and layout choices

A few things in this project differ from the bare request, all for the
sake of conventions you'll see in real Airflow codebases:

- **One name for one thing.** The file is `sample_data.jsonl`, the landing
  table is `raw_sample_data`, the aggregate is `sample_data_totals`, and the
  DAG is `sample_data_pipeline`. The shared stem makes it obvious they
  belong together.
- **Task ids are verb-first and name their target**: `validate_sample_data`,
  `load_raw_sample_data`, `transform_sample_data_totals`. A task list reads
  as a sentence.
- **The connection id says what it is and where it goes**:
  `postgres_airflow_learning`.
- **SQL in `.sql` files, Python in `.py` files**, under `dags/sql/`.
- **Config at the top, in constants.**
- **`schedule=None`** so the DAG only runs when you ask. Change it to
  `"@daily"` once you trust it.
- **`retries=0`** so failures surface immediately while you learn. Real
  pipelines usually set retries with a delay.

## Where to go next

- Change `schedule` to `"@daily"` and watch the scheduler pick it up.
- Add a `customer_totals` table and a second transform task. Fan out from
  `load_raw_sample_data` so both transforms run in parallel.
- Replace the full rebuild in the transform with `INSERT ... ON CONFLICT DO
  UPDATE` and compare the behaviour.
- Move `source_file` into an Airflow Variable and read it with
  `Variable.get`.
- Drop a second JSONL file with new orders into `data/`, trigger with its
  path, and confirm the primary key on `(order_id, source_file)` keeps both
  loads.
