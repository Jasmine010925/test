{{ config(
    materialized='view'
) }}

with emp as (
  select *
  from {{ ref('stg_tensor__employee') }}
  -- Keep just active employees;
  where status = 'active'
),

current_position as (
  /* Prefer an active employment detail per employee;
     if none, fall back to the latest span by last_update/start_date. */
  select distinct on (employee_id)
    employee_id,
    position_id,
    position_start_date,
    position_end_date,
    contracted_hours
  from {{ ref('stg_tensor__employment_details') }}
  order by
    employee_id,
    -- active first
    (case when is_active then 0 else 1 end),
    -- then most recent
    coalesce(last_update, position_start_date) desc nulls last
),

dim_pos as (
  select *
  from {{ ref('stg_tensor__position') }}
),

dim_site as (
  select *
  from {{ ref('stg_tensor__site') }}
  {% if include_site_ids and include_site_ids | length > 0 %}
    where site_id in (
      {# ensure integers even if passed as strings #}
      {%- for id in include_site_ids -%}
        {{ id | int }}{{ "," if not loop.last }}
      {%- endfor -%}
    )
  {% endif %}
),

dim_dept as (
  select *
  from {{ ref('stg_tensor__department') }}
),

final as (
  select
    e.employee_id,
    e.employee_code,
    e.first_name,
    e.last_name,
    -- convenience
    (e.first_name || ' ' || e.last_name) as full_name,

    e.employment_start_date,
    e.employment_end_date,

    cp.position_start_date,
    cp.position_end_date,
    cp.contracted_hours,

    p.job_title,
    p.notes as position_notes,

    s.site_id,
    s.site_code,

    d.department_id,
    d.department_code
  from emp e
  left join current_position cp
    on e.employee_id = cp.employee_id
  left join dim_pos p
    on cp.position_id = p.position_id
  left join dim_site s
    on e.site_id = s.site_id
  left join dim_dept d
    on e.department_id = d.department_id
)

select *
from final
