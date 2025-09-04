WITH all_node_level_5 AS (
    {% set node_Level_5_tables = [
        'kc_pura_vida__Node_Level_5',
        'kc_tim_hortons__Node_Level_5',
        'km_merrychef__Node_Level_5'
    ] %}
    {% for table in node_Level_5_tables %}
    SELECT path, level, nodeid, "nodeName", "parentNodeid", "unitAssignable", "nodeAttributes" FROM {{ source('kitchenconnect', table) }}
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
),

final AS (
    SELECT
        path,
        level,
        nodeid as node_id,
        "nodeName" as node_name,
        "parentNodeid" as parent_node_id,
        "unitAssignable" as unit_assignable,
        "nodeAttributes" ->> 'country' as country,
        "nodeAttributes" ->> 'city' as city,
        "nodeAttributes" ->> 'street' as street,
        "nodeAttributes" ->> 'locationName' as location_name,
        "nodeAttributes" ->> 'zipCode' as zip_code
    FROM all_node_level_5
)

SELECT * FROM final
