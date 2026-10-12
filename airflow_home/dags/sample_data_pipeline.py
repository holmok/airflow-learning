"""
sample_data_pipeline
====================

Load a JSONL file of orders into Postgres and roll it up into totals.

    validate_sample_data ─┐
                          ├─> load_raw_sample_data ─> transform_sample_data_totals ─> check_sample_data_totals
    create_tables ────────┘

See TUTORIAL.md in the project root for a walkthrough.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Iterator, Literal

from pydantic import BaseModel, Field, ValidationError

from airflow.providers.common.sql.operators.sql import (
    SQLCheckOperator,
    SQLExecuteQueryOperator,
)
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# The Airflow connection that points at the airflow_learning database.
# Created once with `airflow connections add` (see TUTORIAL.md step 3).
POSTGRES_CONN_ID = "postgres_airflow_learning"

# This file lives in <project>/airflow_home/dags/, so the project root is
# two directories up. Resolving it here keeps the DAG free of hard-coded
# absolute paths.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SOURCE_FILE = PROJECT_ROOT / "data" / "sample_data.jsonl"

RAW_TABLE = "raw_sample_data"
RAW_COLUMNS = (
    "order_id",
    "customer_id",
    "order_date",
    "region",
    "category",
    "product",
    "quantity",
    "unit_price",
    "source_file",
)

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


class SampleRecord(BaseModel):
    """One line of the JSONL file.

    pydantic does the type coercion ("2026-09-02" -> date, "750.00" ->
    Decimal) and rejects anything that does not fit, so the validate and
    load tasks share one definition of what a good row looks like.
    """

    order_id: str = Field(min_length=1)
    customer_id: str = Field(min_length=1)
    order_date: date
    region: Literal["north", "south", "west"]
    category: Literal["hardware", "software", "services"]
    product: str = Field(min_length=1)
    quantity: int = Field(gt=0)
    unit_price: Decimal = Field(ge=0, decimal_places=2)


def parse_line(line_number: int, line: str) -> SampleRecord:
    """Turn one line of JSONL into a SampleRecord.

    Raises ValueError that names the line number and every field that
    failed, so a validation report reads like "line 4: order_date: ...".
    """
    try:
        payload = json.loads(line)
    except json.JSONDecodeError as exc:
        raise ValueError(f"line {line_number}: not valid JSON ({exc.msg})") from exc
    try:
        return SampleRecord.model_validate(payload)
    except ValidationError as exc:
        problems = "; ".join(
            f"{'.'.join(str(p) for p in err['loc']) or '<root>'}: {err['msg']}"
            for err in exc.errors()
        )
        raise ValueError(f"line {line_number}: {problems}") from exc


def iter_lines(path: Path) -> Iterator[tuple[int, str]]:
    """Yield (line_number, line) for every non-blank line in the file."""
    with path.open() as handle:
        for line_number, line in enumerate(handle, start=1):
            if line.strip():
                yield line_number, line


def read_records(path: Path) -> list[SampleRecord]:
    """Parse the whole file, raising on the first bad line.

    Used by the load task, which only runs after validation has passed,
    so a failure here means the file changed between the two tasks.
    """
    return [parse_line(n, line) for n, line in iter_lines(path)]


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------


@dag(
    dag_id="sample_data_pipeline",
    description="Validate a JSONL file, load it into Postgres, and build totals.",
    schedule=None,  # manual trigger only; set to "@daily" etc. to automate
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 0},  # fail fast while learning; raise this in prod
    params={"source_file": str(DEFAULT_SOURCE_FILE)},
    tags=["tutorial", "postgres"],
    doc_md=__doc__,
)
def sample_data_pipeline():
    @task
    def validate_sample_data(params: dict) -> dict:
        """Read every line, fail if any is malformed, report what was found.

        Nothing is written to the database here. If this task fails the
        rest of the DAG never runs and Postgres is untouched.
        """
        path = Path(params["source_file"])
        if not path.is_file():
            raise FileNotFoundError(f"source file not found: {path}")

        errors: list[str] = []
        row_count = 0
        order_ids: set[str] = set()
        for line_number, line in iter_lines(path):
            try:
                record = parse_line(line_number, line)
            except ValueError as exc:
                errors.append(str(exc))
                if len(errors) >= 20:
                    errors.append("... stopping after 20 errors")
                    break
                continue
            row_count += 1
            if record.order_id in order_ids:
                errors.append(f"line {line_number}: duplicate order_id {record.order_id}")
            order_ids.add(record.order_id)

        if errors:
            raise ValueError(
                f"{path.name} failed validation with {len(errors)} problem(s):\n  "
                + "\n  ".join(errors)
            )

        log.info("%s: %d rows passed validation", path.name, row_count)
        return {"source_file": str(path), "row_count": row_count}

    create_tables = SQLExecuteQueryOperator(
        task_id="create_tables",
        conn_id=POSTGRES_CONN_ID,
        sql="sql/create_tables.sql",
    )

    @task
    def load_raw_sample_data(summary: dict) -> int:
        """Insert the file into raw_sample_data, replacing any earlier load
        of the same file. Delete and insert run in one transaction."""
        path = Path(summary["source_file"])
        rows = [
            (
                r.order_id,
                r.customer_id,
                r.order_date,
                r.region,
                r.category,
                r.product,
                r.quantity,
                r.unit_price,
                path.name,
            )
            for r in read_records(path)
        ]

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        placeholders = ", ".join(["%s"] * len(RAW_COLUMNS))
        insert_sql = (
            f"INSERT INTO {RAW_TABLE} ({', '.join(RAW_COLUMNS)}) VALUES ({placeholders})"
        )
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {RAW_TABLE} WHERE source_file = %s", (path.name,))
                deleted = cur.rowcount
                cur.executemany(insert_sql, rows)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        log.info("replaced %d row(s) with %d row(s) from %s", deleted, len(rows), path.name)
        return len(rows)

    transform_sample_data_totals = SQLExecuteQueryOperator(
        task_id="transform_sample_data_totals",
        conn_id=POSTGRES_CONN_ID,
        sql="sql/transform_sample_data_totals.sql",
    )

    check_sample_data_totals = SQLCheckOperator(
        task_id="check_sample_data_totals",
        conn_id=POSTGRES_CONN_ID,
        sql="sql/check_sample_data_totals.sql",
    )

    # Wiring. validate and create_tables have no dependency on each other,
    # so Airflow runs them in parallel. load waits for both.
    summary = validate_sample_data()
    loaded = load_raw_sample_data(summary)
    create_tables >> loaded
    loaded >> transform_sample_data_totals >> check_sample_data_totals


sample_data_pipeline()
