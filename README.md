# Airflow learning project

Apache Airflow 3.3.1 on macOS, managed with uv, plus a tutorial pipeline
that validates a JSONL file, loads it into Postgres and aggregates it.

| | |
| --- | --- |
| [TUTORIAL.md](TUTORIAL.md) | the pipeline walkthrough, start here once Airflow runs |
| `airflow_home/dags/` | the DAGs and their SQL |
| `data/` | sample JSONL input |
| `scripts/setup.sh` | one-shot bootstrap for a fresh clone |
| `.env.example` | environment variables, copy to `.env` |
| `constraints.txt` | Airflow's dependency pins for 3.3.1 on Python 3.12 |

## Quick start from a clone

You need Homebrew, uv and a running Postgres:

```bash
brew install uv postgresql@18 && brew services start postgresql@18
```

Then:

```bash
git clone <this repo> ~/projects/airflow-learning
cd ~/projects/airflow-learning
./scripts/setup.sh
```

The script creates `.env` from `.env.example`, runs `uv sync` to build
`.venv` from `uv.lock`, creates the `airflow_learning` Postgres role and
database if they are missing, initialises Airflow's metadata database, and
registers the Postgres connection the tutorial DAG uses. It is idempotent,
so re-run it any time.

Start Airflow:

```bash
source .env && source .venv/bin/activate && airflow standalone
```

Open http://localhost:8080. The admin password is in
`airflow_home/simple_auth_manager_passwords.json.generated`. Both DAGs,
`hello` and `sample_data_pipeline`, appear within about 30 seconds.

Every new terminal needs the same two `source` lines. If you use direnv,
an `.envrc` containing `source .env` and `source .venv/bin/activate` does
it automatically.

### What is and isn't in git

Committed: the DAGs, SQL, sample data, `pyproject.toml`, `uv.lock`,
`constraints.txt` and these docs.

Not committed, created on your machine by the setup script or by Airflow
itself: `.venv/`, `.env`, and everything in `airflow_home/` except `dags/`.
That includes `airflow.cfg`, which holds secret keys generated on first
run, the SQLite metadata database, logs and the admin password file.
Airflow connections live in that database, which is why the setup script
registers one rather than git carrying it.

### Doing it by hand

If you would rather not run the script, these are its steps:

```bash
cp .env.example .env && source .env
uv sync && source .venv/bin/activate
psql -U "$(whoami)" -d postgres -c "create role airflow_learning with login password 'airflow_learning'"
psql -U "$(whoami)" -d postgres -c "create database airflow_learning owner airflow_learning"
airflow db migrate
airflow connections add postgres_airflow_learning --conn-type postgres --conn-host localhost --conn-port 5432 --conn-login airflow_learning --conn-password airflow_learning --conn-schema airflow_learning
```

## Adding a dependency

Use `uv add` with the constraints file so Airflow's pins are respected.
With `.env` loaded, `UV_CONSTRAINT` is set and the flag can be omitted:

```bash
uv add "apache-airflow[amazon]==3.3.1"
```

Commit the resulting changes to `pyproject.toml` and `uv.lock`.

---

# How this project was set up from scratch

The rest of this file is the original walkthrough that produced the
repository. You do not need it to use the project, but it explains where
each piece came from and why Airflow is installed the way it is.

Airflow publishes a constraints file per release and Python version that
pins every transitive dependency. Always install with it, or you will
eventually get a broken dependency set.

## 1. Install uv

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Or with Homebrew:

```bash
brew install uv
```

Restart your shell (or `source ~/.zshrc`) and confirm:

```bash
uv --version
```

## 2. Install a supported Python

Airflow 3.3 supports Python 3.10 through 3.13. Use 3.12 unless you have a
reason not to:

```bash
uv python install 3.12
```

## 3. Create the project and its virtual environment

All Airflow packages go into a virtual environment inside the project, not
into the system or Homebrew Python.

```bash
mkdir -p ~/projects/airflow-learning && cd ~/projects/airflow-learning
uv init --python 3.12 --bare
uv venv --python 3.12
source .venv/bin/activate
```

`--bare` creates only a `pyproject.toml`, without a sample `main.py`.
`uv venv` creates `.venv/` in the project. After `source .venv/bin/activate`
your prompt shows `(airflow-learning)` and `which python` points at
`~/projects/airflow-learning/.venv/bin/python`.

Every later step assumes the venv is active. If you open a new terminal:

```bash
cd ~/projects/airflow-learning && source .venv/bin/activate
```

## 4. Pick the version and build the constraints URL

```bash
export AIRFLOW_VERSION=3.3.1
export PYTHON_VERSION=3.12
export CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
```

Check the URL resolves before installing (a 404 means the version pair is
not published):

```bash
curl -sSfI "$CONSTRAINT_URL" | head -1
```

## 5. Install Airflow

Use `uv add`. It records the dependency in `pyproject.toml`, writes a
`uv.lock` with every pinned transitive package, and lets `uv sync` rebuild
the environment from scratch later. `uv add` reads constraints from a local
file rather than a URL, so download the constraints file first:

```bash
curl -sSfL "$CONSTRAINT_URL" -o constraints.txt
uv add "apache-airflow==${AIRFLOW_VERSION}" --constraint constraints.txt
```

After this you should see three things:

- `pyproject.toml` lists `apache-airflow==3.3.1` under `dependencies`
- `uv.lock` exists in the project root
- `constraints.txt` exists in the project root

Commit all three. `uv.lock` is what makes the install reproducible;
`constraints.txt` is what you need the next time you `uv add` a provider.

To avoid retyping `--constraint` on every `uv add` or `uv sync`, point uv
at the file through its environment variable (put it in `.envrc` or
`~/.zshrc`):

```bash
export UV_CONSTRAINT="$HOME/projects/airflow-learning/constraints.txt"
```

Add extras the same way, for example Postgres and Celery:

```bash
uv add "apache-airflow[postgres,celery]==${AIRFLOW_VERSION}" --constraint constraints.txt
```

Verify the install:

```bash
airflow version
```

### Alternative: `uv pip install` (not recommended for this project)

`uv pip install` is a drop-in replacement for `pip install`. It installs into
the active venv and nothing else: no change to `pyproject.toml`, no
`uv.lock`, and no `constraints.txt` on disk since it reads the constraint
straight from the URL.

```bash
uv pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

It is fine for a throwaway experiment, but the project then has no record
of what it depends on and `uv sync` will not restore it. If you already ran
this, running the `uv add` command above on top of it is safe and will
simply write the missing metadata.

## 6. Set AIRFLOW_HOME

Airflow writes its config, SQLite database, logs and `dags/` folder here.
Default is `~/airflow`. To keep it inside the project:

```bash
export AIRFLOW_HOME="$HOME/projects/airflow-learning/airflow_home"
```

In this repo that line lives in `.env.example`. Airflow will silently
create `~/airflow` if you forget to export it.

## 7. Run it

With the venv active, start everything in one process:

```bash
airflow standalone
```

This migrates the database, creates an admin user and starts the
API server, scheduler, dag processor and triggerer. Open
http://localhost:8080.

In Airflow 3 the generated admin password is not printed to the terminal.
Read it from:

```bash
cat "$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated"
```

Stop with Ctrl-C.

## 8. Run components separately (optional)

For anything beyond a first look, run each service in its own terminal:

```bash
airflow db migrate
airflow api-server --port 8080
airflow scheduler
airflow dag-processor
airflow triggerer
```

## 9. Add your first DAG

Airflow only scans the `dags/` folder inside `AIRFLOW_HOME`, which here is
`airflow_home/dags/`, not a `dags/` folder at the project root. Create
`airflow_home/dags/hello.py`:

```python
from airflow.sdk import dag, task
from datetime import datetime

@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False)
def hello():
    @task
    def say_hi():
        print("hi from airflow")
    say_hi()

hello()
```

The dag processor picks it up within about 30 seconds.

## Next: build a real pipeline

Once `hello` shows up in the UI, continue with [TUTORIAL.md](TUTORIAL.md),
which validates a JSONL file, loads it into a local Postgres database and
aggregates it with SQL.

## Alternative: run without a project

If you only want to poke at Airflow, uv can install it as an isolated tool:

```bash
uv tool install "apache-airflow==3.3.1"
airflow standalone
```

or run it once without installing anything:

```bash
uvx apache-airflow standalone
```

Note that `uv tool` and `uvx` do not apply the constraints file, so this
is fine for a demo but not for anything you intend to keep.

## macOS-specific notes

- **Fork safety.** macOS can crash forked workers with
  `objc[...]: +[__NSCFConstantString initialize] may have been in progress
  in another thread when fork() was called`. If you see that, set:

  ```bash
  export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
  ```

- **Port 8080** is sometimes taken by other local services. Use
  `airflow api-server --port 8081` or `airflow standalone` after setting
  `AIRFLOW__API__PORT=8081`.

- **Apple Silicon.** All core wheels ship as arm64. Some provider extras
  (for example `mysql` or `kerberos`) need Homebrew libraries first.
  Install those with `brew` before adding the extra.

- **SQLite** is the default database and is fine locally. Do not use it
  with the Celery or Kubernetes executors.

## Upgrading later

Bump the version, fetch the matching constraints file, re-run `uv add`, then
migrate the database:

```bash
export AIRFLOW_VERSION=3.4.0
export CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
curl -sSfL "$CONSTRAINT_URL" -o constraints.txt
uv add "apache-airflow==${AIRFLOW_VERSION}" --constraint constraints.txt
airflow db migrate
```

Commit the updated `pyproject.toml`, `uv.lock` and `constraints.txt`
together.
