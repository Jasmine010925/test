-- models/staging/kitchenconnect/stg_kitchenconnect__yesterdays_productions.sql

with union_raw as (
  {% set yesterday_productions_tables = [
      'kc_pura_vida__Yesterdays_Productions',
      'kc_tim_hortons__Yesterdays_Productions',
      'km_merrychef__Yesterdays_Productions'
  ] %}

  {% if yesterday_productions_tables | length == 0 %}
    -- No sources resolved -> return an empty, typed table (prevents `with (...)` syntax error)
    select
      null::text        as device_id,
      '[]'::jsonb       as product_details,
      null::timestamptz as extracted_at
    where false
  {% else %}
    {% for table in yesterday_productions_tables %}
      select
        cast("deviceID" as text)        as device_id,
        coalesce(("productDetails")::jsonb, '[]'::jsonb) as product_details,
        (_airbyte_extracted_at)::timestamptz             as extracted_at
      from {{ source('kitchenconnect', table) }}
      --where (_airbyte_extracted_at)::date = (current_date - 1)
      {% if not loop.last %} union all {% endif %}
    {% endfor %}
  {% endif %}
),

expanded as (
  select
    extracted_at::date - 1                as date,
    device_id,
    elem->>'productName'                  as production_name,
    nullif(elem->>'count','')::int        as "count",
    case
      when (elem->>'lastProductTime') ~ '^[0-9]+$' then
        case
          when length(elem->>'lastProductTime') > 10
            then to_timestamp(((elem->>'lastProductTime')::numeric / 1000.0))
          else to_timestamp((elem->>'lastProductTime')::bigint)
        end
      else null
    end                                   as last_production_time,
    extracted_at::date                    as exracted_date
  from union_raw ur
  cross join lateral jsonb_array_elements(ur.product_details) as elem
)

select
  date,
  device_id,
  production_name,
  "count",
  last_production_time
from expanded
