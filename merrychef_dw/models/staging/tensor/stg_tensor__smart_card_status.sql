with src as (
  select *
  from {{ source('tensor', 'tensor__SmartCardStatus') }}
),

-- Normalize names/types
renamed as (
  select
      "StatusID"            as status_id,
      "StatusText"          as status_text,
      _airbyte_raw_id,
      _airbyte_extracted_at,
      _airbyte_generation_id,
      _airbyte_meta
  from src
),

/* If the source can emit multiple rows per status_id, keep the latest by extracted_at.
   DISTINCT ON is Postgres-specific. Remove this CTE if you never get dupes. */
deduped as (
  select distinct on (status_id)
      *
  from renamed
  order by status_id, _airbyte_extracted_at desc nulls last
)

select
    status_id,
    status_text
from deduped