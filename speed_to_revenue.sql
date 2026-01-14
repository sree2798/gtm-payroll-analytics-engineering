SELECT
  employee_band_id,
  lead_channel,
  COUNT(*) AS won_deals,
  AVG(days_first_touch_to_close) AS avg_days_to_close,
  APPROX_QUANTILES(days_first_touch_to_close, 100)[OFFSET(50)] AS median_days_to_close
FROM `analytics.mart_speed_to_revenue`
WHERE is_won = TRUE
GROUP BY 1,2
HAVING won_deals >= 20
ORDER BY avg_days_to_close ASC;
