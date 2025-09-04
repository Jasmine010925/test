with src as (
    select *
    from {{ source('tensor', 'tensor__Supervision') }}
),

filtered as (
    select *
    from src
    where "Deleted" <> 1
),

renamed as (
    select
        "EmployeeID"      as employee_id,
        "SupervisionID"   as supervision_id,
        "SupvEmployeeID"  as supervisor_employee_id
    from filtered
),

final as (
    select *
    from renamed
)

select *
from final
