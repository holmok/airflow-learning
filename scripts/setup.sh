#!/usr/bin/env bash
# One-shot, idempotent setup for a fresh clone. Safe to re-run.
#
#   1. create .env from .env.example if missing
#   2. uv sync  (creates .venv and installs Airflow + provider from uv.lock)
#   3. create the Postgres role and database if missing
#   4. airflow db migrate  (creates airflow_home/airflow.cfg and airflow.db)
#   5. register the postgres_airflow_learning connection if missing
#
# Prereqs: uv, and a running Postgres (brew install postgresql@18 && brew services start postgresql@18)

set -euo pipefail
cd "$(dirname "$0")/.."

step() { printf '\n==> %s\n' "$*"; }

step "environment"
if [[ ! -f .env ]]; then
  cp .env.example .env
  echo "created .env from .env.example (edit it if your paths differ)"
fi
set -a; source .env; set +a
echo "AIRFLOW_HOME=$AIRFLOW_HOME"

step "python environment (uv sync)"
command -v uv >/dev/null || { echo "uv not found: brew install uv"; exit 1; }
uv sync
source .venv/bin/activate

step "postgres"
command -v pg_isready >/dev/null || { echo "pg_isready not on PATH; check the PATH line in .env"; exit 1; }
pg_isready -h "$PGHOST" -p "$PGPORT" >/dev/null || { echo "postgres not running: brew services start postgresql@18"; exit 1; }
# Role and database creation need a superuser, so connect as the Homebrew
# default (your macOS user) to the postgres database, not as PGUSER.
ADMIN=(psql -h "$PGHOST" -p "$PGPORT" -U "$(whoami)" -d postgres -Atq)
if [[ "$("${ADMIN[@]}" -c "select 1 from pg_roles where rolname='$PGUSER'")" != "1" ]]; then
  PGPASSWORD= "${ADMIN[@]}" -c "create role $PGUSER with login password '$PGPASSWORD'"
  echo "created role $PGUSER"
else
  echo "role $PGUSER exists"
fi
if [[ "$("${ADMIN[@]}" -c "select 1 from pg_database where datname='$PGDATABASE'")" != "1" ]]; then
  PGPASSWORD= "${ADMIN[@]}" -c "create database $PGDATABASE owner $PGUSER"
  echo "created database $PGDATABASE"
else
  echo "database $PGDATABASE exists"
fi
psql -c "select 'connected as ' || current_user" -Atq

step "airflow metadata database"
airflow db migrate >/dev/null 2>&1
echo "airflow.cfg and airflow.db ready in $AIRFLOW_HOME"

step "airflow connection"
CONN_ID=postgres_airflow_learning
if airflow connections get "$CONN_ID" >/dev/null 2>&1; then
  echo "connection $CONN_ID exists"
else
  airflow connections add "$CONN_ID" \
    --conn-type postgres \
    --conn-host "$PGHOST" --conn-port "$PGPORT" \
    --conn-login "$PGUSER" --conn-password "$PGPASSWORD" \
    --conn-schema "$PGDATABASE" >/dev/null 2>&1
  echo "created connection $CONN_ID"
fi

step "done"
cat <<MSG
Next, in a new terminal:

  cd $(pwd)
  source .env && source .venv/bin/activate
  airflow standalone

Then open http://localhost:8080 and continue with TUTORIAL.md.
Admin password: cat \$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated
MSG
