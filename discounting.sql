-- Discounting: are we discounting more in certain segments and does it improve close rate?
WITH base AS (
  SELECT
    account_employee_band_label AS segment,
    discount_pct,
    is_won,
    amount_arr,
    (amount_arr * (1 - discount_pct)) AS net_arr
  FROM `analytics.fact_opportunity_enriched`
),
bucketed AS (
  SELECT
    segment,
    CASE
      WHEN discount_pct < 0.10 THEN '0-10%'
      WHEN discount_pct < 0.20 THEN '10-20%'
      WHEN discount_pct < 0.30 THEN '20-30%'
      ELSE '30%+'
    END AS discount_bucket,
    COUNT(*) AS opps,
    SUM(CASE WHEN is_won THEN 1 ELSE 0 END) AS wins,
    SAFE_DIVIDE(SUM(CASE WHEN is_won THEN 1 ELSE 0 END), COUNT(*)) AS win_rate,
    AVG(discount_pct) AS avg_discount,
    AVG(net_arr) AS avg_net_arr
  FROM bucketed
  GROUP BY 1,2
)
SELECT *
FROM bucketed
ORDER BY segment, discount_bucket;
