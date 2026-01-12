select
  cast(stage_event_id as string) as stage_event_id,
  cast(opp_id as string) as opp_id,
  cast(stage_name as string) as stage_name,
  cast(entered_at as timestamp) as entered_at,
  cast(exited_at as timestamp) as exited_at
from {{ source('raw','crm_opportunity_stage_history') }}
