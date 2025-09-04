with src as (
    select *
    from {{ source('tensor', 'tensor__EmployeeMiscTextItem') }}
),

renamed as (
    select
        "EmployeeMiscTextItemID" as employee_misc_text_item_id,
        "MiscText"               as misc_text
    from src
),

final as (
    select *
    from renamed
)

select *
from final
