DECLARE start_date DATE DEFAULT DATE('2024-01-01');
DECLARE end_date   DATE DEFAULT DATE('2025-12-31');
DECLARE n_accounts INT64 DEFAULT 10000;
DECLARE n_leads    INT64 DEFAULT 50000;
DECLARE n_opps     INT64 DEFAULT 30000;
DECLARE n_campaigns INT64 DEFAULT 200;

CREATE SCHEMA IF NOT EXISTS `raw`;

CREATE OR REPLACE TABLE `raw.crm_accounts` AS
WITH base AS (
  SELECT
    x AS seq,
    RAND() AS r
  FROM UNNEST(GENERATE_ARRAY(1, n_accounts)) x
)
SELECT
  FORMAT("A%05d", seq) AS account_id,
  CONCAT("Company_", CAST(seq AS STRING)) AS account_name,
  CAST(
    CASE
      WHEN r < 0.20  THEN 1 + CAST(FLOOR(RAND() * 10) AS INT64)
      WHEN r < 0.50  THEN 11 + CAST(FLOOR(RAND() * 40) AS INT64)  
      WHEN r < 0.75  THEN 51 + CAST(FLOOR(RAND() * 150) AS INT64)
      WHEN r < 0.88  THEN 201 + CAST(FLOOR(RAND() * 300) AS INT64)
      WHEN r < 0.95  THEN 501 + CAST(FLOOR(RAND() * 500) AS INT64)
      WHEN r < 0.99  THEN 1001 + CAST(FLOOR(RAND() * 4000) AS INT64)        
      WHEN r < 0.995 THEN 5001 + CAST(FLOOR(RAND() * 5000) AS INT64) 
      ELSE               10001 + CAST(FLOOR(RAND() * 15000) AS INT64)
    END AS INT64
  ) AS employee_count,
  (SELECT AS STRUCT ARRAY_AGG(ind ORDER BY RAND() LIMIT 1)[OFFSET(0)] ind
   FROM UNNEST(["Healthcare","Manufacturing","Retail","Tech","Hospitality","Education","Finance","Nonprofit","Construction"]) ind).ind AS industry,
  (SELECT AS STRUCT ARRAY_AGG(rg ORDER BY RAND() LIMIT 1)[OFFSET(0)] rg
   FROM UNNEST(["Midwest","Northeast","South","West"]) rg).rg AS region,
  (SELECT AS STRUCT ARRAY_AGG(ch ORDER BY RAND() LIMIT 1)[OFFSET(0)] ch
   FROM UNNEST(["inbound","outbound","partner","events","referral"]) ch).ch AS acquisition_channel,
  TIMESTAMP(DATE_ADD(start_date, INTERVAL CAST(FLOOR(RAND() * (DATE_DIFF(end_date, start_date, DAY)+1)) AS INT64) DAY)) AS created_at,
  0 AS is_customer
FROM base;

CREATE OR REPLACE TABLE `raw.marketing_campaigns` AS
SELECT
  FORMAT("CAMP%03d", c) AS campaign_id,
  CONCAT("Campaign_", CAST(c AS STRING)) AS campaign_name,
  (SELECT AS STRUCT ARRAY_AGG(ch ORDER BY RAND() LIMIT 1)[OFFSET(0)] ch
   FROM UNNEST(["paid_search","paid_social","webinar","content","events","partner_marketing"]) ch).ch AS channel,
  DATE_ADD(start_date, INTERVAL CAST(FLOOR(RAND()*400) AS INT64) DAY) AS campaign_start_date,
  DATE_ADD(start_date, INTERVAL 30 + CAST(FLOOR(RAND()*500) AS INT64) DAY) AS campaign_end_date,
  (SELECT AS STRUCT ARRAY_AGG(obj ORDER BY RAND() LIMIT 1)[OFFSET(0)] obj
   FROM UNNEST(["demand_gen","pipeline_accel","brand","partner_growth"]) obj).obj AS objective
FROM UNNEST(GENERATE_ARRAY(1, n_campaigns)) c;

CREATE OR REPLACE TABLE `raw.marketing_spend_daily`
PARTITION BY spend_date
CLUSTER BY campaign_id AS
WITH days AS (
  SELECT d AS spend_date
  FROM UNNEST(GENERATE_DATE_ARRAY(start_date, end_date)) d
)
SELECT
  days.spend_date,
  c.campaign_id,
  c.channel,
  ROUND(
    CASE c.channel
      WHEN "paid_search" THEN 300 + RAND()*700
      WHEN "paid_social" THEN 250 + RAND()*650
      WHEN "events" THEN 100 + RAND()*900
      WHEN "webinar" THEN 80 + RAND()*250
      WHEN "partner_marketing" THEN 60 + RAND()*220
      ELSE 50 + RAND()*180
    END
  , 2) AS spend_usd
FROM `raw.marketing_campaigns` c
CROSS JOIN days
WHERE days.spend_date BETWEEN c.campaign_start_date AND c.campaign_end_date;

CREATE OR REPLACE TABLE `raw.marketing_leads` AS
WITH base AS (
  SELECT
    x AS seq,
    RAND() AS r,
    TIMESTAMP(DATE_ADD(start_date, INTERVAL CAST(FLOOR(RAND() * (DATE_DIFF(end_date, start_date, DAY)+1)) AS INT64) DAY)) AS created_at
  FROM UNNEST(GENERATE_ARRAY(1, n_leads)) x
),
camp_pick AS (
  SELECT
    campaign_id
  FROM `raw.marketing_campaigns`
)
SELECT
  FORMAT("L%06d", seq) AS lead_id,
  created_at,
  (SELECT AS STRUCT ARRAY_AGG(ch ORDER BY RAND() LIMIT 1)[OFFSET(0)] ch
   FROM UNNEST(["paid_search","paid_social","organic","partner","event","referral"]) ch).ch AS lead_channel,
  (SELECT AS STRUCT ARRAY_AGG(campaign_id ORDER BY RAND() LIMIT 1)[OFFSET(0)] campaign_id
   FROM camp_pick).campaign_id AS campaign_id,
  CAST(
    CASE
      WHEN r < 0.22  THEN 1 + CAST(FLOOR(RAND() * 10) AS INT64)
      WHEN r < 0.52  THEN 11 + CAST(FLOOR(RAND() * 40) AS INT64)
      WHEN r < 0.78  THEN 51 + CAST(FLOOR(RAND() * 150) AS INT64)
      WHEN r < 0.89  THEN 201 + CAST(FLOOR(RAND() * 300) AS INT64)
      WHEN r < 0.95  THEN 501 + CAST(FLOOR(RAND() * 500) AS INT64)
      WHEN r < 0.985 THEN 1001 + CAST(FLOOR(RAND() * 4000) AS INT64)
      WHEN r < 0.995 THEN 5001 + CAST(FLOOR(RAND() * 5000) AS INT64)
      ELSE               10001 + CAST(FLOOR(RAND() * 15000) AS INT64)
    END AS INT64
  ) AS employee_count_estimate,
  (SELECT AS STRUCT ARRAY_AGG(ind ORDER BY RAND() LIMIT 1)[OFFSET(0)] ind
   FROM UNNEST(["Healthcare","Manufacturing","Retail","Tech","Hospitality","Education","Finance","Nonprofit","Construction"]) ind).ind AS industry,
  (SELECT AS STRUCT ARRAY_AGG(rg ORDER BY RAND() LIMIT 1)[OFFSET(0)] rg
   FROM UNNEST(["Midwest","Northeast","South","West"]) rg).rg AS region,
  IF(RAND() < 0.70, TRUE, FALSE) AS is_mql,
  IF(RAND() < 0.40, TRUE, FALSE) AS is_sql,
  IF(RAND() < 0.30,
     FORMAT("A%05d", 1 + CAST(FLOOR(RAND() * n_accounts) AS INT64)),
     NULL
  ) AS converted_account_id
FROM base;

CREATE OR REPLACE TABLE `raw.marketing_touches`
PARTITION BY DATE(touch_ts)
CLUSTER BY lead_id AS
WITH touches AS (
  SELECT
    l.lead_id,
    l.created_at,
    1 + CAST(FLOOR(RAND() * 4) AS INT64) AS n_touches
  FROM `raw.marketing_leads` l
)
SELECT
  CONCAT("TCH_", lead_id, "_", CAST(i AS STRING)) AS touch_id,
  lead_id,
  TIMESTAMP_ADD(created_at, INTERVAL CAST(FLOOR(RAND()*60*24*30) AS INT64) MINUTE) AS touch_ts,
  (SELECT AS STRUCT ARRAY_AGG(tt ORDER BY RAND() LIMIT 1)[OFFSET(0)] tt
   FROM UNNEST(["email","ad_click","web_visit","webinar_attended","event_booth","demo_request"]) tt).tt AS touch_type
FROM touches
CROSS JOIN UNNEST(GENERATE_ARRAY(1, touches.n_touches)) i;

CREATE OR REPLACE TABLE `raw.crm_opportunities` AS
WITH opp_base AS (
  SELECT
    x AS seq,
    FORMAT("A%05d", 1 + CAST(FLOOR(RAND() * n_accounts) AS INT64)) AS account_id,
    TIMESTAMP(DATE_ADD(start_date, INTERVAL CAST(FLOOR(RAND() * (DATE_DIFF(end_date, start_date, DAY)+1)) AS INT64) DAY)) AS created_at
  FROM UNNEST(GENERATE_ARRAY(1, n_opps)) x
),
with_acct AS (
  SELECT
    o.*,
    a.employee_count
  FROM opp_base o
  JOIN `raw.crm_accounts` a USING(account_id)
),
scored AS (
  SELECT
    *,
    CASE
      WHEN employee_count BETWEEN 1 AND 10 THEN 0.22
      WHEN employee_count BETWEEN 11 AND 50 THEN 0.28
      WHEN employee_count BETWEEN 51 AND 200 THEN 0.35
      WHEN employee_count BETWEEN 201 AND 500 THEN 0.38
      WHEN employee_count BETWEEN 501 AND 1000 THEN 0.36
      WHEN employee_count BETWEEN 1001 AND 5000 THEN 0.30
      WHEN employee_count BETWEEN 5001 AND 10000 THEN 0.26
      ELSE 0.18
    END AS win_prob
  FROM with_acct
),
final AS (
  SELECT
    FORMAT("O%06d", seq) AS opp_id,
    account_id,
    created_at,
    TIMESTAMP_ADD(created_at, INTERVAL CAST(10 + FLOOR(RAND()*120) AS INT64) DAY) AS closed_at,
    CAST(GREATEST(2000, CAST(EXP(8 + RAND()*4) AS INT64)) AS INT64) AS amount_arr,
    ROUND(LEAST(GREATEST(0.0, 0.10 + (RAND()-0.5)*0.22), 0.45), 3) AS discount_pct,
    IF(RAND() < win_prob, TRUE, FALSE) AS is_won,
    (SELECT AS STRUCT ARRAY_AGG(comp ORDER BY RAND() LIMIT 1)[OFFSET(0)] comp
     FROM UNNEST(["ADP","Paychex","Gusto","UKG","Ceridian","Rippling","Other","None"]) comp).comp AS primary_competitor,
    (SELECT AS STRUCT ARRAY_AGG(src ORDER BY RAND() LIMIT 1)[OFFSET(0)] src
     FROM UNNEST(["inbound","outbound","partner","events","referral"]) src).src AS source_channel
  FROM scored
)
SELECT
  *,
  IF(is_won, "Closed Won", "Closed Lost") AS stage
FROM final;

CREATE OR REPLACE TABLE `raw.crm_opportunity_stage_history`
PARTITION BY DATE(entered_at)
CLUSTER BY opp_id AS
WITH stages AS (
  SELECT
    opp_id,
    created_at,
    closed_at,
    stage AS final_stage
  FROM `raw.crm_opportunities`
),
expanded AS (
  SELECT
    opp_id,
    created_at,
    closed_at,
    final_stage,
    3 + CAST(FLOOR(RAND() * 3) AS INT64) AS n_steps
  FROM stages
),
stage_list AS (
  SELECT
    opp_id,
    created_at,
    closed_at,
    final_stage,
    n_steps,
    ["Prospecting","Qualified","Discovery","Proposal","Negotiation"] AS path
  FROM expanded
),
row_count AS (
  SELECT
    opp_id,
    created_at,
    closed_at,
    final_stage,
    i AS step_idx,
    path[OFFSET(i-1)] AS stage_name,
    n_steps
  FROM stage_list
  CROSS JOIN UNNEST(GENERATE_ARRAY(1, n_steps)) i
),
timed AS (
  SELECT
    opp_id,
    stage_name,
    TIMESTAMP_ADD(created_at, INTERVAL CAST(FLOOR((step_idx-1) * (TIMESTAMP_DIFF(closed_at, created_at, SECOND) / n_steps)) AS INT64) SECOND) AS entered_at,
    TIMESTAMP_ADD(created_at, INTERVAL CAST(FLOOR((step_idx)   * (TIMESTAMP_DIFF(closed_at, created_at, SECOND) / n_steps)) AS INT64) SECOND) AS exited_at,
    step_idx,
    n_steps
  FROM row_count
),
final AS (
  SELECT * FROM timed
  UNION ALL
  SELECT
    o.opp_id,
    o.stage AS stage_name,
    o.closed_at AS entered_at,
    NULL AS exited_at,
    999 AS step_idx,
    999 AS n_steps
  FROM `raw.crm_opportunities` o
)
SELECT
  CONCAT("OSH_", opp_id, "_", CAST(ABS(FARM_FINGERPRINT(CAST(entered_at AS STRING))) AS STRING)) AS stage_event_id,
  opp_id,
  stage_name,
  entered_at,
  exited_at
FROM final;

CREATE OR REPLACE TABLE `raw.billing_contracts` AS
WITH won AS (
  SELECT
    *
  FROM `raw.crm_opportunities`
  WHERE is_won = TRUE
),
enriched AS (
  SELECT
    w.*,
    a.employee_count
  FROM won w
  JOIN `raw.crm_accounts` a USING(account_id)
)
SELECT
  CONCAT("CTR_", opp_id) AS contract_id,
  account_id,
  DATE(TIMESTAMP_ADD(closed_at, INTERVAL CAST(FLOOR(RAND()*30) AS INT64) DAY)) AS start_date,
  DATE_ADD(
    DATE(TIMESTAMP_ADD(closed_at, INTERVAL CAST(FLOOR(RAND()*30) AS INT64) DAY)),
    INTERVAL (12 * (1 + CAST(FLOOR(RAND()*2) AS INT64))) MONTH
  ) AS end_date,
  CASE
    WHEN employee_count <= 50 THEN (SELECT AS STRUCT ARRAY_AGG(b ORDER BY RAND() LIMIT 1)[OFFSET(0)] b
                                   FROM UNNEST(["Payroll Only","Payroll+Time"]) b).b
    WHEN employee_count <= 500 THEN (SELECT AS STRUCT ARRAY_AGG(b ORDER BY RAND() LIMIT 1)[OFFSET(0)] b
                                    FROM UNNEST(["Payroll+Time","Payroll+Time+Benefits","Full HCM"]) b).b
    ELSE (SELECT AS STRUCT ARRAY_AGG(b ORDER BY RAND() LIMIT 1)[OFFSET(0)] b
          FROM UNNEST(["Payroll+Time+Benefits","Full HCM"]) b).b
  END AS product_bundle,
  CAST(
    (amount_arr * (1 - discount_pct)) *
    CASE
      WHEN RAND() < 0.25 THEN 1.00
      WHEN RAND() < 0.70 THEN 1.05
      ELSE 1.12
    END
    AS INT64
  ) AS arr
FROM enriched;

UPDATE `raw.crm_accounts` a
SET is_customer = 1
WHERE EXISTS (
  SELECT 1 FROM `raw.billing_contracts` c
  WHERE c.account_id = a.account_id
);

CREATE OR REPLACE TABLE `raw.billing_invoices`
PARTITION BY invoice_date
CLUSTER BY account_id AS
WITH months AS (
  SELECT
    contract_id,
    account_id,
    d AS invoice_date,
    arr
  FROM `raw.billing_contracts`,
  UNNEST(GENERATE_DATE_ARRAY(start_date, DATE_SUB(end_date, INTERVAL 1 DAY), INTERVAL 1 MONTH)) d
)
SELECT
  CONCAT("INV_", contract_id, "_", FORMAT_DATE("%Y%m", invoice_date)) AS invoice_id,
  account_id,
  contract_id,
  invoice_date,
  ROUND(arr/12.0, 2) AS amount_usd,
  IF(RAND() < 0.96, "Paid", "Past Due") AS status
FROM months;

CREATE OR REPLACE TABLE `raw.product_usage_daily`
PARTITION BY usage_date
CLUSTER BY account_id AS
WITH customers AS (
  SELECT
    c.account_id,
    c.product_bundle,
    a.employee_count,
    c.start_date AS contract_start_date
  FROM `raw.billing_contracts` c
  JOIN `raw.crm_accounts` a USING(account_id)
),
days AS (
  SELECT d AS usage_date
  FROM UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(end_date, INTERVAL 179 DAY), end_date)) d
)
SELECT
  days.usage_date,
  cust.account_id,
  IF(
    EXTRACT(DAYOFWEEK FROM days.usage_date) IN (5,6)
    AND RAND() < CASE
      WHEN cust.employee_count <= 50 THEN 0.18
      WHEN cust.employee_count <= 500 THEN 0.25
      WHEN cust.employee_count <= 5000 THEN 0.30
      ELSE 0.34
    END,
    1, 0
  ) AS payroll_runs,
  CAST(GREATEST(1, ROUND(LOG(cust.employee_count + 1) * 2 + (RAND()-0.5)*2)) AS INT64) AS active_admins,
  IF(STRPOS(cust.product_bundle, "Time") > 0, 1, 0) AS adopt_time,
  IF(STRPOS(cust.product_bundle, "Benefits") > 0, 1, 0) AS adopt_benefits,
  IF(cust.product_bundle = "Full HCM", 1, 0) AS adopt_full_hcm
FROM customers cust
CROSS JOIN days;

CREATE OR REPLACE TABLE `raw.support_tickets`
PARTITION BY DATE(created_at)
CLUSTER BY account_id AS
WITH cust AS (
  SELECT DISTINCT
    a.account_id,
    a.employee_count
  FROM `raw.crm_accounts` a
  WHERE a.is_customer = 1
),
base AS (
  SELECT
    c.account_id,
    c.employee_count,
    CASE
      WHEN employee_count <= 50 THEN 2 + CAST(FLOOR(RAND()*2) AS INT64)
      WHEN employee_count <= 500 THEN 3 + CAST(FLOOR(RAND()*3) AS INT64)
      ELSE 4 + CAST(FLOOR(RAND()*3) AS INT64)
    END AS n_tickets
  FROM cust c
)
SELECT
  CONCAT(
    "SUP_", b.account_id, "_",
    CAST(ABS(FARM_FINGERPRINT(CONCAT(b.account_id, "_", CAST(i AS STRING), "_", CAST(RAND() AS STRING)))) AS STRING)
  ) AS ticket_id,
  b.account_id,
  TIMESTAMP(DATE_ADD(start_date, INTERVAL CAST(FLOOR(RAND() * (DATE_DIFF(end_date, start_date, DAY)+1)) AS INT64) DAY)) AS created_at,
  (SELECT AS STRUCT ARRAY_AGG(s ORDER BY RAND() LIMIT 1)[OFFSET(0)] s
   FROM UNNEST(["Low","Med","High","Sev1"]) s).s AS severity,
  CAST(GREATEST(1, ROUND(EXP(2 + RAND()*2))) AS INT64) AS resolution_hours,
  IF(RAND() < 0.70, 1 + CAST(FLOOR(RAND()*5) AS INT64), NULL) AS csat
FROM base b
CROSS JOIN UNNEST(GENERATE_ARRAY(1, b.n_tickets)) i;

--  check the data and counts
SELECT 'crm_accounts' AS table_name, COUNT(*) AS rows FROM `raw.crm_accounts`
UNION ALL SELECT 'marketing_campaigns', COUNT(*) FROM `raw.marketing_campaigns`
UNION ALL SELECT 'marketing_spend_daily', COUNT(*) FROM `raw.marketing_spend_daily`
UNION ALL SELECT 'marketing_leads', COUNT(*) FROM `raw.marketing_leads`
UNION ALL SELECT 'marketing_touches', COUNT(*) FROM `raw.marketing_touches`
UNION ALL SELECT 'crm_opportunities', COUNT(*) FROM `raw.crm_opportunities`
UNION ALL SELECT 'crm_opportunity_stage_history', COUNT(*) FROM `raw.crm_opportunity_stage_history`
UNION ALL SELECT 'billing_contracts', COUNT(*) FROM `raw.billing_contracts`
UNION ALL SELECT 'billing_invoices', COUNT(*) FROM `raw.billing_invoices`
UNION ALL SELECT 'product_usage_daily', COUNT(*) FROM `raw.product_usage_daily`
UNION ALL SELECT 'support_tickets', COUNT(*) FROM `raw.support_tickets`;
