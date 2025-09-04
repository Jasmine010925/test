{{ config(
    materialized='view'
) }}

select *
from {{ ref('inc_table_size') }}
