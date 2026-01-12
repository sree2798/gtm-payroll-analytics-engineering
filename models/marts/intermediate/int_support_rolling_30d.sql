with d as (
  select date_day from {{ ref('dim_date') }}
),

tickets as (
  select
    created_date as date_day,
    account_id,
    severity
  from {{ ref('fact_support_ticket') }}
),

daily as (
  select
    d.date_day,
    a.account_id,
    count(t.account_id) as tickets_created,
    sum(case when t.severity = 'Sev1' then 1 else 0 end) as sev1_tickets_created
  from d
  -- build grid for all accounts/day (so windows work consistently)
  cross join (select distinct account_id from {{ ref('dim_account') }}) a
  left join tickets t
    on t.account_id = a.account_id
   and t.date_day = d.date_day
  group by 1,2
),

roll as (
  select
    date_day,
    account_id,
    sum(tickets_created) over (
      partition by account_id
      order by date_day
      rows between 29 preceding and current row
    ) as tickets_30d,
    sum(sev1_tickets_created) over (
      partition by account_id
      order by date_day
      rows between 29 preceding and current row
    ) as sev1_tickets_30d
  from daily
)

select
  *,
  safe_divide(sev1_tickets_30d, nullif(tickets_30d, 0)) as sev1_rate_30d
from roll
