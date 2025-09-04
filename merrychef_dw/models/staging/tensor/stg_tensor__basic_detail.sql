with src as (
    select *
    from {{ source('tensor', 'tensor__BasicDetail') }}
),

filtered as (
    select *
    from src
    where "Deleted" <> 1
),

renamed as (
    select
        "EmployeeID"       as employee_id,
        "DOB"              as date_of_birth,
        "Country"          as country_id,
        'NINXXX'           as national_insurance_number,  -- Placeholder for privacy
        "KnownAs"          as known_as,
        "Address1"         as address_1,
        "Address2"         as address_2,
        "Address3"         as address_3,
        "Address4"         as address_4,
        "MobileNo"         as mobile_number,
        "PostCode"         as post_code,
        "EmailHome"        as home_email
    from filtered
),

final as (
    select *
    from renamed
    order by employee_id
)

select *
from final
