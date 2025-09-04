with src as (
  select *
  from {{ source('tensor', 'tensor__Employee') }}
),

-- Keep only non-deleted rows (tolerate NULLs)
filtered as (
  select *
  from src
  where coalesce("Deleted", 0) = 0
),

-- Normalize types, names, and derive status
renamed as (
  select
    "AccessGroupType"      as access_group_type,
    "BradfordIndex"        as bradford_index,
    "CompanyID"            as company_id,
    "DateOfBirth"::date as date_of_birth,
    "DefaultFirePointID"   as default_fire_point_id,
    "DefaultShiftGroupID"  as default_shift_group_id,
    "DepartmentID"         as department_id,
    "EmailOffice"          as email_office,
    "EmployeeCode"         as employee_code,
    "EmployeeID"           as employee_id,
    "EmploymentEndDate"::date   as employment_end_date,
    "EmploymentStartDate"::date as employment_start_date,
    "EmploymentType"       as employment_type,
    "FirstName"            as first_name,
    "FRCStatus"            as frc_status,
    "Gender"               as gender,
    "GroupOffsetType"      as group_offset_type,
    "LastName"             as last_name,
    "LastUpdate"           as last_update,        -- keep original type; often timestamp
    "MiddleName"           as middle_name,
    "Misc1"                as misc1,
    "Misc2"                as misc2,
    "PayClassID"           as pay_class_id,
    "PaymentInterval"      as payment_interval,
    "PayrollNumber"        as payroll_number,
    "SectionID"            as section_id,
    "SiteID"               as site_id,
    "Title"                as title,

    case
      when "EmploymentEndDate" is null or "EmploymentEndDate"::date >= current_date
        then 'active'
      else 'inactive'
    end as status
  from filtered
),

-- If the source can emit multiple rows per employee, keep the latest by last_update.
-- DISTINCT ON is efficient and Postgres-specific.
deduped as (
  select distinct on (employee_id)
    *
  from renamed
  order by employee_id, last_update desc nulls last
)

select *
from deduped
