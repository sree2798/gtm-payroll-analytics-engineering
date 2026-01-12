select
  lead_id,
  created_at,
  created_date,
  lead_channel,
  campaign_id,
  employee_count_estimate,
  industry,
  region,
  is_mql,
  is_sql,
  converted_account_id
from {{ ref('stg_marketing_leads') }}
