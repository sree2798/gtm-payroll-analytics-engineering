select
  cast(touch_id as string) as touch_id,
  cast(lead_id as string) as lead_id,
  cast(touch_ts as timestamp) as touch_ts,
  date(cast(touch_ts as timestamp)) as touch_date,
  cast(touch_type as string) as touch_type
from {{ source('raw','marketing_touches') }}
