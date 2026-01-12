with a as (
  select * from {{ ref('stg_crm__accounts') }}
),

bands as (
  select * from {{ ref('dim_employee_band') }}
),

banded as (
  select
    a.*,
    b.band_id as employee_band_id,
    b.band_label as employee_band_label
  from a
  left join bands b
    on a.employee_count >= b.min_employees
   and (b.max_employees is null or a.employee_count <= b.max_employees)
)

select * from banded
