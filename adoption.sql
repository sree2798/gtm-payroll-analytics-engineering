WITH usage AS (
  SELECT
    account_id,
    ANY_VALUE(employee_band_label) AS segment,
    AVG(payroll_runs_30d) AS payroll_runs_30d,
    AVG(active_admins_30d) AS active_admins_30d,
    AVG(adopt_time) AS adopt_time,
    AVG(adopt_benefits) AS adopt_benefits,
    AVG(adopt_full_hcm) AS adopt_full_hcm
  FROM `analytics.mart_gtm_account_daily`
  GROUP BY 1
),
ret AS (
  SELECT
    account_id,
    AVG(nrr) AS nrr
  FROM `analytics.mart_retention_monthly`
  GROUP BY 1
)
SELECT
  u.segment,
  COUNT(*) AS accounts,
  CORR(u.payroll_runs_30d, r.nrr) AS corr_payroll_runs_to_nrr,
  CORR(u.active_admins_30d, r.nrr) AS corr_admins_to_nrr,
  CORR(u.adopt_time, r.nrr) AS corr_time_to_nrr,
  CORR(u.adopt_benefits, r.nrr) AS corr_benefits_to_nrr,
  CORR(u.adopt_full_hcm, r.nrr) AS corr_fullhcm_to_nrr
FROM usage u
JOIN ret r USING(account_id)
GROUP BY 1
ORDER BY accounts DESC;
