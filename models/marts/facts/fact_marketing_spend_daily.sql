select
  spend_date,
  campaign_id,
  channel,
  spend_usd
from {{ ref('stg_marketing_spend_daily') }}
