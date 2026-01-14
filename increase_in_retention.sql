-- adopted vs non adopted
WITH adoption AS (
  SELECT
    account_id,
    ANY_VALUE(employee_band_label) AS segment,
    MAX(adopt_time) AS adopt_time,
    MAX(adopt_benefits) AS adopt_benefits,
    MAX(adopt_full_hcm) AS adopt_full_hcm
  FROM `analytics.mart_gtm_account_daily`
  GROUP BY 1
),
ret AS (
  SELECT
    account_id,
    AVG(nrr) AS nrr,
    AVG(grr) AS grr
  FROM `analytics.mart_retention_monthly`
  GROUP BY 1
)
SELECT
  segment,
  AVG(CASE WHEN adopt_time=1 THEN r.nrr END) AS nrr_time_adopters,
  AVG(CASE WHEN adopt_time=0 THEN r.nrr END) AS nrr_time_nonadopters,
  AVG(CASE WHEN adopt_benefits=1 THEN r.nrr END) AS nrr_benefits_adopters,
  AVG(CASE WHEN adopt_benefits=0 THEN r.nrr END) AS nrr_benefits_nonadopters,
  AVG(CASE WHEN adopt_full_hcm=1 THEN r.nrr END) AS nrr_fullhcm_adopters,
  AVG(CASE WHEN adopt_full_hcm=0 THEN r.nrr END) AS nrr_fullhcm_nonadopters
FROM adoption a
JOIN ret r USING(account_id)
GROUP BY 1
ORDER BY segment;
