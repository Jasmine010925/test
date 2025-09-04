WITH items AS (
    SELECT
        item_number,
        description1 as description
    FROM {{ ref('stg_qad__pt_mstr') }}
    WHERE item_number IS NOT NULL

),

final as (
    select
        *
    FROM items
)

SELECT * FROM final
