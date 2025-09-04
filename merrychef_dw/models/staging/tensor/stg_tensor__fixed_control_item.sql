
with src as (
    select *
    from {{ source('tensor', 'tensor__FixedControlItem') }}
),

filtered as (
    select *
    from src
    where "Deleted" <> 1
),

renamed as (
    select
        "FixedControlItemID" as item_id,
        "ControlID"          as control_id,
        "ControlValue"       as control_value
    from filtered
),

final as (
    select *
    from renamed
)

select *
from final