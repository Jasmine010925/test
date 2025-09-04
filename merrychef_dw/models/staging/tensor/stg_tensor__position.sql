with src as (
  select *
  from {{ source('tensor', 'tensor__Position') }}
),

-- Soft-delete guard (tolerate NULLs)
filtered as (
  select *
  from src
  where coalesce("Deleted", 0) = 0
),

-- Normalize names/types and do light cleanup
renamed as (
  select
    "PositionID"::int                 as position_id,
    trim("PositionCode")              as position_code,
    trim("JobTitle")                  as job_title,
    nullif(trim("Notes"), '')         as notes
  from filtered
)

select *
from renamed
