with src as (
    select *
    from {{ source('tensor', 'tensor__Country') }}
),

filtered as (
    select *
    from src
    where "Deleted" <> 1
),

renamed as (
    select
        "CountryID"       as country_id,
        "LastUpdate"      as last_update,
        "CountryCode"     as country_code,
        "CountryName"     as country_name
    from filtered
),

final as (
    select *
    from renamed
)

select *
from final
