with leads as (
  select
    created_date as date_day,
    campaign_id,
    lead_channel,
    converted_account_id as account_id,
    is_mql,
    is_sql,
    lead_id
  from {{ ref('fact_marketing_lead') }}
),

lead_to_opp as (
  select
    lead_id,
    opp_id,
    account_id,
    date(lead_created_at) as date_day,
    campaign_id,
    lead_channel
  from {{ ref('int_lead_to_opportunity') }}
),

acct as (
  select account_id, employee_band_id, employee_band_label
  from {{ ref('dim_account') }}
),

agg as (
  select
    l.date_day,
    l.campaign_id,
    l.lead_channel,
    a.employee_band_id,
    a.employee_band_label,

    count(*) as leads,
    sum(case when l.is_mql then 1 else 0 end) as mqls,
    sum(case when l.is_sql then 1 else 0 end) as sqls,
    count(distinct l2o.opp_id) as opps
  from leads l
  left join acct a
    on a.account_id = l.account_id
  left join lead_to_opp l2o
    on l2o.lead_id = l.lead_id
  group by 1,2,3,4,5
)

select * from agg
