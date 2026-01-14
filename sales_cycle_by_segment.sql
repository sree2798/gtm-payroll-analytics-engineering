SELECT
  account_employee_band_label AS segment,
  source_channel,
  COUNT(*) AS opps,
  AVG(sales_cycle_days) AS avg_sales_cycle_days,
  APPROX_QUANTILES(sales_cycle_days, 100)[OFFSET(50)] AS median_sales_cycle_days
FROM `analytics.fact_opportunity_enriched`
GROUP BY 1,2
HAVING opps >= 50
ORDER BY avg_sales_cycle_days DESC;
