select
  cast(lead_id as string) as lead_id,
  cast(created_at as timestamp) as created_at,
  date(cast(created_at as timestamp)) as created_date,

  cast(lead_channel as string) as lead_channel,
  cast(campaign_id as string) as campaign_id,

  cast(employee_count_estimate as int64) as employee_count_estimate,
  cast(industry as string) as industry,
  cast(region as string) as region,

  cast(is_mql as bool) as is_mql,
  cast(is_sql as bool) as is_sql,

  cast(converted_account_id as string) as converted_account_id
from {{ source('raw','marketing_leads') }}
