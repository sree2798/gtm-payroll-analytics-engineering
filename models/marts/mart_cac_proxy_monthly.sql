with spend as (
  select
    format_date('%Y-%m', spend_date) as year_month,
    campaign_id,
    channel,
    sum(spend_usd) as spend_usd
  from {{ ref('fact_marketing_spend_daily') }}
  group by 1,2,3
),

funnel as (
  select
    format_date('%Y-%m', date_day) as year_month,
    campaign_id,
    lead_channel,
    employee_band_id,
    sum(opps) as opps
  from {{ ref('mart_marketing_funnel_daily') }}
  group by 1,2,3,4
),

wins as (
  select
    format_date('%Y-%m', date(l.lead_created_at)) as year_month,
    l.campaign_id,
    l.lead_channel,
    a.employee_band_id,
    countif(o.is_won) as wins
  from {{ ref('int_lead_to_opportunity') }} l
  join {{ ref('fact_opportunity') }} o using (opp_id)
  left join {{ ref('dim_account') }} a on a.account_id = l.account_id
  group by 1,2,3,4
)

select
  s.year_month,
  s.campaign_id,
  s.channel,
  f.employee_band_id,
  s.spend_usd,
  coalesce(f.opps, 0) as opps,
  coalesce(w.wins, 0) as wins,
  safe_divide(s.spend_usd, nullif(f.opps, 0)) as cost_per_opp,
  safe_divide(s.spend_usd, nullif(w.wins, 0)) as cost_per_win
from spend s
left join funnel f
  on f.year_month = s.year_month
 and f.campaign_id = s.campaign_id
left join wins w
  on w.year_month = s.year_month
 and w.campaign_id = s.campaign_id
