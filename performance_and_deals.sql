-- Rep performance
SELECT
  rep_name,
  account_employee_band_label AS segment,
  COUNT(*) AS opps,
  SAFE_DIVIDE(SUM(CASE WHEN is_won THEN 1 ELSE 0 END), COUNT(*)) AS win_rate,
  AVG(sales_cycle_days) AS avg_cycle_days,
  AVG(amount_arr) AS avg_arr
FROM `analytics.fact_opportunity_enriched`
GROUP BY 1,2
HAVING opps >= 30
ORDER BY win_rate DESC, avg_arr DESC;

-- stall points
SELECT
  stage_name,
  AVG(avg_days_in_stage) AS avg_days_in_stage
FROM `analytics.mart_stage_stall`
GROUP BY 1
ORDER BY avg_days_in_stage DESC;
