with src as (
    select *
    from {{ source('tensor', 'tensor__Company') }}
),

filtered as (
    select *
    from src
    where "Deleted" <> 1
),

renamed as (
    select
        "CompanyID"                     as company_id,
        "LastUpdate"                   as last_update,
        "CompanyCode"                  as company_code,
        "CompanyName"                  as company_name,
        "FriendlyName"                 as friendly_name,
        "StaffGroupID"                 as staff_group_id,
        "CompanyGroupID"               as company_group_id,
        "CarryOverUsedBy"              as carry_over_used_by,
        "IsSubContractor"              as is_sub_contractor,
        "ParentCompanyID"              as parent_company_id,
        "EnableCompanyWTR"             as enable_company_wtr,
        "UnavailableForVMS"            as unavailable_for_vms,
        "DefaultAccessGroupID"         as default_access_group_id,
        "UnusedHolidayCarryOver"       as unused_holiday_carry_over,
        "OvertimeRequiresApproval"     as overtime_requires_approval,
        "HolidayCarryOverDebitDays"    as holiday_carry_over_debit_days,
        "HolidayCarryOverDebitMinutes" as holiday_carry_over_debit_minutes,
        "HolidayCarryOverCreditMinutes"as holiday_carry_over_credit_minutes
    from filtered
),

final as (
    select *
    from renamed
)

select *
from final
