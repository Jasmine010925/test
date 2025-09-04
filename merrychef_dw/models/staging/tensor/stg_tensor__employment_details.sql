with src as (
  select *
  from {{ source('tensor', 'tensor__EmploymentDetails') }}
),

renamed as (
  select
    "EmployeeID"              as employee_id,
    "Position"                as position_id,           -- keep their "Position" as an ID per your mapping
    "LastUpdate"              as last_update,           -- often a timestamp
    "PositionStartDate"::date as position_start_date,
    "PositionVacatedDate"::date as position_end_date,
    "ContractedHours"::numeric as contracted_hours,

    -- Optional: derive a simple active flag
    case
      when "PositionVacatedDate" is null or "PositionVacatedDate"::date >= current_date
        then true
      else false
    end as is_active
  from src
),

/* If the source can emit multiple rows per (employee_id, position_id),
   keep the latest by last_update (fall back to start date).
   DISTINCT ON is efficient and Postgres-specific.
*/
deduped as (
  select distinct on (employee_id, position_id)
    *
  from renamed
  order by employee_id, position_id,
           coalesce(last_update, position_start_date) desc nulls last
)

select
  employee_id,
  position_id,
  last_update,
  position_start_date,
  position_end_date,
  contracted_hours,
  is_active
from deduped
