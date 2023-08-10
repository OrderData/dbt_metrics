{{ config(materialized='table') }}

with days as (
    {{ metrics.metric_date_spine(
    datepart="minute",
    start_date="cast('1990-01-01' as date)",
    end_date="cast('2030-01-01' as date)"
   )
    }}
),

final as (
    select 
        date_minute as date_minute,
        cast({{ date_trunc('hour', 'date_minute') }} as date) as date_hour
    from days
)

select * from final
