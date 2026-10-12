-- Full rebuild. SQLExecuteQueryOperator runs this file inside a single
-- transaction, so readers never see an empty or half-written table.
DELETE FROM sample_data_totals;

INSERT INTO sample_data_totals (
    region,
    category,
    order_count,
    total_quantity,
    total_revenue,
    first_order_date,
    last_order_date
)
SELECT
    region,
    category,
    COUNT(*)                      AS order_count,
    SUM(quantity)                 AS total_quantity,
    SUM(quantity * unit_price)    AS total_revenue,
    MIN(order_date)               AS first_order_date,
    MAX(order_date)               AS last_order_date
FROM raw_sample_data
GROUP BY region, category
ORDER BY region, category;
