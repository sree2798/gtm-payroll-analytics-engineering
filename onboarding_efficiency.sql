SELECT
  employee_band_label AS segment,
  COUNT(*) AS accounts,
  AVG(time_to_first_payroll_days) AS avg_days,
  APPROX_QUANTILES(time_to_first_payroll_days, 100)[OFFSET(50)] AS median_days
FROM `analytics.mart_onboarding`
GROUP BY 1
ORDER BY avg_days DESC;
