WITH support AS (
  SELECT
    account_id,
    ANY_VALUE(employee_band_label) AS segment,
    AVG(employee_count) AS employee_count,
    AVG(ticket_count_30d) AS tickets_30d,
    AVG(sev1_rate_30d) AS sev1_rate_30d
  FROM `analytics.mart_gtm_account_daily`
  GROUP BY 1
),
ret AS (
  SELECT
    account_id,
    AVG(nrr) AS nrr,
    SUM(churn_arr) AS churn_arr
  FROM `analytics.mart_retention_monthly`
  GROUP BY 1
)
SELECT
  s.segment,
  COUNT(*) AS accounts,
  AVG(100 * s.tickets_30d / NULLIF(s.employee_count,0)) AS tickets_per_100_emp,
  AVG(s.sev1_rate_30d) AS avg_sev1_rate,
  AVG(r.nrr) AS avg_nrr,
  SUM(r.churn_arr) AS total_churn_arr
FROM support s
LEFT JOIN ret r USING(account_id)
GROUP BY 1
ORDER BY total_churn_arr DESC;
