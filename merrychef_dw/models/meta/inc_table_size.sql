{{ config(
    materialized='incremental',
    unique_key='dbt_tracking_id'
) }}

with base_table_sizes as (

    select
        current_database() as database_name,
        pt.schemaname as schema_name,
        pt.tablename as table_name,
        pg_total_relation_size(format('%I.%I', pt.schemaname, pt.tablename)) as total_bytes,
        date_trunc('minute', now()) as recorded_at,
        coalesce(psut.n_live_tup, 0) as estimated_rows,
        case
            when c.relkind = 'r' then 'table'
            when c.relkind = 'v' then 'view'
            when c.relkind = 'm' then 'materialized_view'
            when c.relkind = 'f' then 'foreign_table'
            when c.relkind = 'p' then 'partitioned_table'
            else 'other'
        end as object_type
    from
        pg_tables pt
    left join
        pg_stat_user_tables psut
        on psut.schemaname = pt.schemaname
        and psut.relname = pt.tablename
    left join
        pg_class c
        on c.relname = pt.tablename
    left join
        pg_namespace n
        on n.oid = c.relnamespace
        and n.nspname = pt.schemaname
    where
        pt.schemaname in (
            'airbyte_internal', 'clensed', 'datamart', 'dw', 'intermediate', 'manual', 'meta', 'ml', 'raw', 'serving'
        )

), 

table_sizes as (
    select *,
        {{ dbt_utils.generate_surrogate_key(['schema_name', 'table_name', 'recorded_at']) }} as dbt_tracking_id
    from base_table_sizes
)

{% if is_incremental() %}

, latest_existing as (
    select max(recorded_at) as max_time
    from {{ this }}
)

, filtered_new as (
    select *
    from table_sizes
    where recorded_at > (select max_time from latest_existing)
)

select * from filtered_new

{% else %}

select * from table_sizes

{% endif %}
