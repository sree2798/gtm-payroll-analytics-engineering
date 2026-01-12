select
  cast(spend_date as date) as spend_date,
  cast(campaign_id as string) as campaign_id,
  cast(channel as string) as channel,
  cast(spend_usd as float64) as spend_usd
from {{ source('raw','marketing_spend_daily') }}
