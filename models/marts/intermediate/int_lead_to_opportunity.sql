with leads as (
  select * from {{ ref('fact_marketing_lead') }}
  where converted_account_id is not null
),

opps as (
  select
    opp_id,
    account_id,
    created_at as opp_created_at,
    closed_at as opp_closed_at,
    is_won
  from {{ ref('fact_opportunity') }}
),

candidates as (
  select
    l.lead_id,
    l.campaign_id,
    l.lead_channel,
    l.created_at as lead_created_at,
    l.converted_account_id as account_id,
    o.opp_id,
    o.opp_created_at,
    o.opp_closed_at,
    o.is_won,
    row_number() over (
      partition by l.lead_id
      order by o.opp_created_at asc
    ) as rn
  from leads l
  join opps o
    on o.account_id = l.converted_account_id
   and o.opp_created_at >= l.created_at
   and o.opp_created_at < timestamp_add(l.created_at, interval 180 day)
)

select * except(rn)
from candidates
where rn = 1
