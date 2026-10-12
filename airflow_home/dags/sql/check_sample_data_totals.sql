-- SQLCheckOperator fails the task if any column in the first row is
-- false, zero, null or empty. Each column is one assertion.
SELECT
    (SELECT COUNT(*) FROM sample_data_totals) > 0
        AS totals_not_empty,
    (SELECT COALESCE(SUM(total_revenue), 0) FROM sample_data_totals)
      = (SELECT COALESCE(SUM(quantity * unit_price), 0) FROM raw_sample_data)
        AS revenue_matches_raw,
    (SELECT COALESCE(SUM(order_count), 0) FROM sample_data_totals)
      = (SELECT COUNT(*) FROM raw_sample_data)
        AS order_count_matches_raw;
