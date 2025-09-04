with src as (
  select *
  from {{ source('tensor', 'tensor__Site') }}
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
    trim("MapURL")                         as map_url,
    "SiteID"::int                          as site_id,
    -- keep the raw Deleted flag only if you need lineage/debugging; otherwise omit it
    coalesce("Deleted", 0)::int            as deleted,

    upper(nullif(trim("Postcode"), ''))    as postcode,
    "RegionID"::int                        as region_id,
    trim("SiteCode")                       as site_code,
    coalesce("CCSMember", false)           as ccs_member,
    "CompanyID"::int                       as company_id,
    "DaylightID"::int                      as daylight_id,
    "LastUpdate"                           as last_update,
    "TEPOptions"::int                      as tep_options,
    trim("Description")                    as description,
    nullif(trim("AddressLine1"), '')       as address_line_1,
    nullif(trim("AddressLine2"), '')       as address_line_2,
    nullif(trim("AddressLine3"), '')       as address_line_3,
    nullif(trim("AddressLine4"), '')       as address_line_4,
    trim("FriendlyName")                   as friendly_name,

    "AccessGroupID"::int                   as access_group_id,
    case when coalesce("EnableSiteWTR", 0)::int = 1 then true else false end as enable_site_wtr,
    "InductionType"::int                  as induction_type,
    "ScannerThemeID"::int                  as scanner_theme_id,
    "TimeZoneOffset"::int                  as time_zone_offset,         -- often minutes from UTC
    trim("AccessGroupDesc")                as access_group_desc,
    "AccessGroupType"::int                as access_group_type,
    "DefaultCardType"::int                as default_card_type,
    case when coalesce("NonHardwareSite", 0)::int = 1 then true else false end as non_hardware_site,
    case when coalesce("UnavailableForVMS", 0)::int = 1 then true else false end as unavailable_for_vms,
    "ControllerDateFormat"           as controller_date_format,
    "DefaultVisitorFirePoint"::int         as default_visitor_fire_point,
    "DefaultEmployeeFirePoint"::int        as default_employee_fire_point,
    case when coalesce("FilterDuplexingForDevices", 0)::int = 1 then true else false end as filter_duplexing_for_devices,
    "DefaultContractorFirePoint"::int      as default_contractor_fire_point,

    -- Keep raw timestamps for potential de-dupe
    "_airbyte_extracted_at"                  as _airbyte_extracted_at
  from filtered
),

/* If the source can emit multiple rows per site, keep the latest by last_update (or emitted_at).
   DISTINCT ON is efficient and Postgres-specific. Remove this CTE if you never get dupes. */
deduped as (
  select distinct on (site_id)
    *
  from renamed
  order by site_id, coalesce(last_update, _airbyte_extracted_at) desc nulls last
)

select
  map_url,
  site_id,
  postcode,
  region_id,
  site_code,
  ccs_member,
  company_id,
  daylight_id,
  last_update,
  tep_options,
  description,
  address_line_1, address_line_2, address_line_3, address_line_4,
  friendly_name,
  access_group_id,
  enable_site_wtr,
  induction_type,
  scanner_theme_id,
  time_zone_offset,
  access_group_desc,
  access_group_type,
  default_card_type,
  non_hardware_site,
  unavailable_for_vms,
  controller_date_format,
  default_visitor_fire_point,
  default_employee_fire_point,
  filter_duplexing_for_devices,
  default_contractor_fire_point
from deduped
