-- Landing table: one row per line in the JSONL file, typed but otherwise
-- untouched. Metadata columns record where and when each row came from.
CREATE TABLE IF NOT EXISTS raw_sample_data (
    order_id     TEXT           NOT NULL,
    customer_id  TEXT           NOT NULL,
    order_date   DATE           NOT NULL,
    region       TEXT           NOT NULL,
    category     TEXT           NOT NULL,
    product      TEXT           NOT NULL,
    quantity     INTEGER        NOT NULL CHECK (quantity > 0),
    unit_price   NUMERIC(10, 2) NOT NULL CHECK (unit_price >= 0),
    source_file  TEXT           NOT NULL,
    loaded_at    TIMESTAMPTZ    NOT NULL DEFAULT now(),
    PRIMARY KEY (order_id, source_file)
);

-- Aggregate table rebuilt from raw_sample_data on every run.
CREATE TABLE IF NOT EXISTS sample_data_totals (
    region           TEXT           NOT NULL,
    category         TEXT           NOT NULL,
    order_count      INTEGER        NOT NULL,
    total_quantity   INTEGER        NOT NULL,
    total_revenue    NUMERIC(12, 2) NOT NULL,
    first_order_date DATE           NOT NULL,
    last_order_date  DATE           NOT NULL,
    computed_at      TIMESTAMPTZ    NOT NULL DEFAULT now(),
    PRIMARY KEY (region, category)
);
