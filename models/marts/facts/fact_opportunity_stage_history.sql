with sh as (
  select * from {{ ref('stg_crm_opportunity_stage_history') }}
),
o as (
  select opp_id, closed_at from {{ ref('stg_crm__opportunities') }}
)

select
  sh.stage_event_id,
  sh.opp_id,
  sh.stage_name,
  sh.entered_at,
  coalesce(sh.exited_at, o.closed_at) as exited_at_effective,
  greatest(
    timestamp_diff(coalesce(sh.exited_at, o.closed_at), sh.entered_at, second),
    0
  ) as time_in_stage_seconds
from sh
left join o using (opp_id)
