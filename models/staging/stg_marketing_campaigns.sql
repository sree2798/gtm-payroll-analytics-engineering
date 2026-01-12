select
  cast(campaign_id as string) as campaign_id,
  cast(campaign_name as string) as campaign_name,
  cast(channel as string) as channel,
  cast(campaign_start_date as date) as campaign_start_date,
  cast(campaign_end_date as date) as campaign_end_date,
  cast(objective as string) as objective
from {{ source('raw','marketing_campaigns') }}
