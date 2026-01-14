SELECT
  account_employee_band_label AS segment,
  primary_competitor,
  COUNT(*) AS opps,
  SUM(CASE WHEN is_won THEN 1 ELSE 0 END) AS wins,
  SAFE_DIVIDE(SUM(CASE WHEN is_won THEN 1 ELSE 0 END), COUNT(*)) AS win_rate,
  AVG(amount_arr) AS avg_arr
FROM `analytics.fact_opportunity_enriched`
GROUP BY 1,2
HAVING opps >= 50
ORDER BY segment, win_rate DESC;
