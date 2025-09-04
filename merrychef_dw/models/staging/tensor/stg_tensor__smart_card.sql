with src as (
  select *
  from {{ source('tensor', 'tensor__SmartCard') }}
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
      "Type"               as type,
      "Status"             as status,
      "HexNumber"          as hex_number,
      "CardFamily"         as card_family,
      "CardFormat"         as card_format,
      "EmployeeID"         as employee_id,
      "LastUpdate"         as last_update,
      "DisableCard"        as disable_card,
      "NotifyOnUse"        as notify_on_use,
      "SmartCardID"        as smartcard_id,
      "EmbossedNumber"     as embossed_number,
      "InternalNumber"     as internal_number,
      "InternalNumber32"   as internal_number32,
      _airbyte_raw_id,
      _airbyte_extracted_at,
      _airbyte_generation_id,
      _airbyte_meta
  from filtered
),

/* If the source can emit multiple rows per card, keep the latest by last_update (or extracted_at).
   DISTINCT ON is efficient and Postgres-specific. Remove this CTE if you never get dupes. */
deduped as (
  select distinct on (smartcard_id)
      *
  from renamed
  order by smartcard_id, coalesce(last_update, _airbyte_extracted_at) desc nulls last
)

select
    type,
    status,
    hex_number,
    card_family,
    card_format,
    employee_id,
    last_update,
    disable_card,
    notify_on_use,
    smartcard_id,
    embossed_number,
    internal_number,
    internal_number32
from deduped

