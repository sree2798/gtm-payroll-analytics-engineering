SELECT
  employee_band_id,
  AVG(grr) AS avg_grr,
  AVG(nrr) AS avg_nrr,
  SUM(churn_arr) AS churn_arr
FROM `analytics.mart_retention_monthly`
GROUP BY 1
ORDER BY churn_arr DESC;
