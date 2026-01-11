with source as (
  select * from {{ source('raw', 'crm_opportunities') }}
),

renamed as (
  select
    cast(opp_id as string) as opp_id,
    cast(account_id as string) as account_id,
    cast(created_at as timestamp) as created_at,
    cast(closed_at as timestamp) as closed_at,

    cast(amount_arr as int64) as amount_arr,
    cast(discount_pct as float64) as discount_pct,
    cast(is_won as bool) as is_won,

    cast(primary_competitor as string) as primary_competitor,
    cast(source_channel as string) as source_channel,
    cast(stage as string) as stage
  from source
)

select * from renamed
