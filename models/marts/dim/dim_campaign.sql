select
  campaign_id,
  campaign_name,
  channel,
  campaign_start_date,
  campaign_end_date,
  objective
from {{ ref('stg_marketing_campaigns') }}
