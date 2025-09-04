{{ config(
    materialized='view'
) }}

WITH smart_cards AS (
    SELECT
        status,
        employee_id,
        hex_number
    FROM {{ ref('stg_tensor__smart_card') }}
    WHERE 'status' IS NOT NULL
),

employees AS (
    SELECT
        employee_id,
        first_name,
        last_name,
        employee_code
    FROM {{ ref('stg_tensor__employee') }}
),

card_status AS (
    SELECT
        status_id,
        status_text
    FROM {{ ref('stg_tensor__smart_card_status') }}
    WHERE 'status_id' IS NOT NULL
),

final AS (
    SELECT
        asc2.employee_id,
        asc2.hex_number,
        ae.first_name,
        ae.last_name,
        ae.employee_code,
        ascs.status_text as card_status
    FROM smart_cards asc2
    LEFT JOIN employees ae on ae.employee_id = asc2.employee_id
    LEFT JOIN card_status ascs on ascs.status_id = asc2.status
)

Select * from final