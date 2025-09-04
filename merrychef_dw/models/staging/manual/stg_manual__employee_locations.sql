with src as (
    select *
    from {{ source('manual', 'employee_locations') }}
),

renamed as (
    select
        location,
        division,
        office
    from src
),

final as (
    select *
    from renamed
)

select *
from final
