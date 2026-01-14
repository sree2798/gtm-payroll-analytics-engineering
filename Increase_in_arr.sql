SELECT
  account_employee_band_label AS segment,
  product_bundle,
  COUNT(DISTINCT account_id) AS customers,
  AVG(arr) AS avg_arr,
  SUM(arr) AS total_arr
FROM `analytics.fact_contract_enriched`
GROUP BY 1,2
ORDER BY segment, total_arr DESC;
