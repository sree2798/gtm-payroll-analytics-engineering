with touches as (
  select
    lead_id,
    min(touch_ts) as first_touch_ts
  from {{ ref('fact_marketing_touch') }}
  group by 1
),

l2o as (
  select * from {{ ref('int_lead_to_opportunity') }}
),

o as (
  select opp_id, closed_at, is_won
  from {{ ref('fact_opportunity') }}
),

acct as (
  select account_id, employee_band_id
  from {{ ref('dim_account') }}
)

select
  l2o.campaign_id,
  l2o.lead_channel,
  a.employee_band_id,
  l2o.opp_id,
  o.is_won,
  timestamp_diff(o.closed_at, t.first_touch_ts, day) as days_first_touch_to_close
from l2o
join touches t using (lead_id)
join o using (opp_id)
left join acct a on a.account_id = l2o.account_id
where o.closed_at is not null
