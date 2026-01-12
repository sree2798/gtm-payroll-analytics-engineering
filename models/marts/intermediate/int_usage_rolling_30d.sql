with u as (
  select
    usage_date as date_day,
    account_id,
    payroll_runs,
    active_admins,
    adopt_time,
    adopt_benefits,
    adopt_full_hcm
  from {{ ref('fact_product_usage_daily') }}
),

roll as (
  select
    date_day,
    account_id,
    sum(payroll_runs) over (
      partition by account_id
      order by date_day
      rows between 29 preceding and current row
    ) as payroll_runs_30d,
    avg(active_admins) over (
      partition by account_id
      order by date_day
      rows between 29 preceding and current row
    ) as avg_active_admins_30d,
    max(adopt_time) over (partition by account_id order by date_day rows between unbounded preceding and current row) as adopt_time,
    max(adopt_benefits) over (partition by account_id order by date_day rows between unbounded preceding and current row) as adopt_benefits,
    max(adopt_full_hcm) over (partition by account_id order by date_day rows between unbounded preceding and current row) as adopt_full_hcm
  from u
)

select * from roll
