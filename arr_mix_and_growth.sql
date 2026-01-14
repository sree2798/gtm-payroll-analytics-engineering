SELECT
  DATE_TRUNC(date_day, MONTH) AS month,
  employee_band_label AS segment,
  SUM(current_arr) AS arr
FROM `analytics.mart_gtm_account_daily`
GROUP BY 1,2
ORDER BY month, arr DESC;
