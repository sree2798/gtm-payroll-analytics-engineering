-- Question: enough pipeline to hit next quarter ARR target by segment?
-- Assumption: pipeline coverage = created pipeline next quarter vs target ARR.
WITH next_q AS (
  SELECT
    DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL 1 QUARTER), QUARTER) AS q_start,
    DATE_SUB(DATE_ADD(DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL 1 QUARTER), QUARTER), INTERVAL 1 QUARTER), INTERVAL 1 DAY) AS q_end
),
pipeline AS (
  SELECT
    account_employee_band_label AS segment,
    SUM(amount_arr) AS pipeline_created_arr
  FROM `analytics.fact_opportunity_enriched`, next_q
  WHERE DATE(created_at) BETWEEN q_start AND q_end
  GROUP BY 1
),
arr AS (
  SELECT
    employee_band_label AS segment,
    SUM(current_arr) AS current_arr
  FROM `analytics.mart_gtm_account_daily`, next_q
  WHERE date_day = (SELECT q_start FROM next_q)
  GROUP BY 1
)
SELECT
  p.segment,
  p.pipeline_created_arr,
  a.current_arr,
  SAFE_DIVIDE(p.pipeline_created_arr, a.current_arr) AS pipeline_to_arr_ratio,
  SAFE_DIVIDE(p.pipeline_created_arr, a.current_arr * 3) AS coverage_vs_3x_rule
FROM pipeline p
LEFT JOIN arr a USING(segment)
ORDER BY pipeline_created_arr DESC;
