with d as (
  select date_day from {{ ref('dim_date') }}
),

contracts as (
  select * from {{ ref('fact_contract') }}
),

arr_daily as (
  select
    d.date_day,
    c.account_id,
    sum(c.arr) as current_arr
  from d
  join contracts c
    on d.date_day >= c.start_date
   and d.date_day < c.end_date
  group by 1,2
)

select * from arr_daily
