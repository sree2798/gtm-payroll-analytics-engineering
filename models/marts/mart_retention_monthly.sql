with arr_daily as (
  select
    date_day,
    account_id,
    current_arr
  from {{ ref('mart_gtm_account_daily') }}
),

monthly as (
  select
    format_date('%Y-%m', date_day) as year_month,
    account_id,
    -- start ARR = first day in month, end ARR = last day in month
    any_value(current_arr) over (
      partition by account_id, format_date('%Y-%m', date_day)
      order by date_day
      rows between unbounded preceding and unbounded following
    ) as arr_any,
    first_value(current_arr) over (
      partition by account_id, format_date('%Y-%m', date_day)
      order by date_day
    ) as start_arr,
    last_value(current_arr) over (
      partition by account_id, format_date('%Y-%m', date_day)
      order by date_day
      rows between unbounded preceding and unbounded following
    ) as end_arr
  from arr_daily
),

dedup as (
  select distinct year_month, account_id, start_arr, end_arr
  from monthly
),

with_prev as (
  select
    d.*,
    lag(end_arr) over (partition by account_id order by year_month) as prev_end_arr
  from dedup d
),

calc as (
  select
    year_month,
    account_id,
    prev_end_arr as beginning_arr,
    end_arr as ending_arr,

    greatest(end_arr - prev_end_arr, 0) as expansion_arr,
    greatest(prev_end_arr - end_arr, 0) as contraction_arr,

    case when prev_end_arr > 0 and end_arr = 0 then prev_end_arr else 0 end as churn_arr
  from with_prev
  where prev_end_arr is not null
),

acct as (
  select account_id, employee_band_id
  from {{ ref('dim_account') }}
)

select
  c.year_month,
  a.employee_band_id,

  sum(c.beginning_arr) as beginning_arr,
  sum(c.ending_arr) as ending_arr,
  sum(c.expansion_arr) as expansion_arr,
  sum(c.contraction_arr) as contraction_arr,
  sum(c.churn_arr) as churn_arr,

  safe_divide(sum(c.beginning_arr - c.contraction_arr - c.churn_arr), nullif(sum(c.beginning_arr),0)) as grr,
  safe_divide(sum(c.beginning_arr + c.expansion_arr - c.contraction_arr - c.churn_arr), nullif(sum(c.beginning_arr),0)) as nrr
from calc c
left join acct a using (account_id)
group by 1,2
