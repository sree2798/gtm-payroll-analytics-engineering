with sh as (
  select * from {{ ref('fact_opportunity_stage_history') }}
),
o as (
  select
    opp_id,
    account_id,
    rep_id,
    source_channel,
    primary_competitor,
    is_won
  from {{ ref('fact_opportunity') }}
),
a as (
  select account_id, employee_band_id
  from {{ ref('dim_account') }}
)

select
  sh.stage_name,
  o.rep_id,
  o.source_channel,
  o.primary_competitor,
  o.is_won,
  a.employee_band_id,
  avg(sh.time_in_stage_seconds) / 86400.0 as avg_days_in_stage,
  count(*) as stage_events
from sh
join o using (opp_id)
left join a using (account_id)
group by 1,2,3,4,5,6
 