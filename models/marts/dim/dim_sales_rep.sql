with reps as (
  select 1 as rep_id, 'Rep 01' as rep_name union all
  select 2, 'Rep 02' union all
  select 3, 'Rep 03' union all
  select 4, 'Rep 04' union all
  select 5, 'Rep 05' union all
  select 6, 'Rep 06' union all
  select 7, 'Rep 07' union all
  select 8, 'Rep 08' union all
  select 9, 'Rep 09' union all
  select 10,'Rep 10' union all
  select 11,'Rep 11' union all
  select 12,'Rep 12' union all
  select 13,'Rep 13' union all
  select 14,'Rep 14' union all
  select 15,'Rep 15'
)
select * from reps
