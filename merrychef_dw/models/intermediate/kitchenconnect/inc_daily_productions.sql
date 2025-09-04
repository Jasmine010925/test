{{ 
  config(
    materialized = 'incremental',
    unique_key = ['date','device_id','production_name'],
    incremental_strategy = 'merge',
    on_schema_change = 'sync_all_columns'
  ) 
}}

-- Source = your staging view that emits exactly yesterday
with src as (
  select
    date,
    device_id,
    production_name,
    coalesce("count", 0) as count,
    last_production_time
  from {{ ref('stg_kitchenconnect__yesterdays_productions') }}
),

-- Optional: protect against duplicate rows per device/product (some brands may duplicate)
dedup as (
  select
    date,
    device_id,
    production_name,
    sum(count)::int                    as count,                 -- daily total for that device/product
    max(last_production_time)          as last_production_time   -- last time seen yesterday
  from src
  group by 1,2,3
)

select * from dedup
