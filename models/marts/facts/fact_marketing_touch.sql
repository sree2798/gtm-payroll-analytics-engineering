select
  touch_id,
  lead_id,
  touch_ts,
  touch_date,
  touch_type
from {{ ref('stg_marketing_touches') }}
