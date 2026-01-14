SELECT
  employee_band_id,
  channel,
  SUM(spend_usd) AS spend,
  SUM(opps) AS opps,
  SUM(wins) AS wins,
  SAFE_DIVIDE(SUM(spend_usd), SUM(opps)) AS cost_per_opp,
  SAFE_DIVIDE(SUM(spend_usd), SUM(wins)) AS cost_per_win
FROM `analytics.mart_cac_proxy_monthly`
GROUP BY 1,2
HAVING opps > 0
ORDER BY cost_per_win DESC;
