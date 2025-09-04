with src as (
    select *
    from {{ source('tensor', 'tensor__EmployeeMiscText') }}
),

renamed as (
    select
        "EmployeeID"             as employee_id,
        "EmployeeMiscTextID"     as employee_misc_text_id,
        "EmployeeMiscTextItemID" as employee_misc_text_item_id,
        "MiscColumn"             as misc_column
    from src
),

final as (
    select *
    from renamed
)

select *
from final
