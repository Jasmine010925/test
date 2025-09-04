with src as (
  select *
  from {{ source('tensor', 'tensor__Department') }}
),

-- Soft-delete guard (tolerate NULLs)
filtered as (
  select *
  from src
  where coalesce("Deleted", 0) = 0
),

-- Normalize names & types; keep it explicit
renamed as (
  select
    "DepartmentID"   ::int   as department_id,
    trim("DepartmentCode")   as department_code,
    trim("Description")      as department_name
  from filtered
)

select *
from renamed
