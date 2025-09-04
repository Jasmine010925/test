WITH items AS (
    SELECT
        ad_name as supplier_name,
        ad_addr as supplier_number
    FROM {{ ref('stg_qad__ad_mstr') }}
    WHERE ad_type ILIKE 'supplier'
),

final as (
    select
        *
    FROM items
)

SELECT * FROM final
