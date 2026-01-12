with d as (
  select date_day from {{ ref('dim_date') }}
),

accounts as (
  select
    account_id,
    industry,
    region,
    acquisition_channel,
    employee_count,
    employee_band_id,
    employee_band_label
  from {{ ref('dim_account') }}
),

arr as (
  select * from {{ ref('int_account_arr_daily') }}
),

pipe as (
  select * from {{ ref('int_pipeline_daily') }}
),

usage30 as (
  select * from {{ ref('int_usage_rolling_30d') }}
),

support30 as (
  select * from {{ ref('int_support_rolling_30d') }}
),

grid as (
  select
    d.date_day,
    a.*
  from d
  cross join accounts a
)

select
  g.date_day,
  g.account_id,
  g.industry,
  g.region,
  g.acquisition_channel,
  g.employee_count,
  g.employee_band_id,
  g.employee_band_label,

  coalesce(arr.current_arr, 0) as current_arr,

  coalesce(pipe.pipeline_created_arr, 0) as pipeline_created_arr,
  coalesce(pipe.won_arr, 0) as won_arr,

  coalesce(usage30.payroll_runs_30d, 0) as payroll_runs_30d,
  coalesce(usage30.avg_active_admins_30d, 0) as avg_active_admins_30d,
  coalesce(usage30.adopt_time, 0) as adopt_time,
  coalesce(usage30.adopt_benefits, 0) as adopt_benefits,
  coalesce(usage30.adopt_full_hcm, 0) as adopt_full_hcm,

  coalesce(support30.tickets_30d, 0) as tickets_30d,
  coalesce(support30.sev1_tickets_30d, 0) as sev1_tickets_30d,
  support30.sev1_rate_30d

from grid g
left join arr
  on arr.date_day = g.date_day and arr.account_id = g.account_id
left join pipe
  on pipe.date_day = g.date_day and pipe.account_id = g.account_id
left join usage30
  on usage30.date_day = g.date_day and usage30.account_id = g.account_id
left join support30
  on support30.date_day = g.date_day and support30.account_id = g.account_id
