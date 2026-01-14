SELECT
  employee_band_label AS segment,
  lead_channel,
  SUM(leads) AS leads,
  SUM(mqls) AS mqls,
  SUM(sqls) AS sqls,
  SUM(opps) AS opps,
  SAFE_DIVIDE(SUM(mqls), SUM(leads)) AS mql_rate,
  SAFE_DIVIDE(SUM(sqls), SUM(mqls)) AS sql_rate,
  SAFE_DIVIDE(SUM(opps), SUM(leads)) AS lead_to_opp_rate
FROM `analytics.mart_marketing_funnel_daily`
GROUP BY 1,2
ORDER BY lead_to_opp_rate DESC;
