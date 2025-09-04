with employees as (
    select  
        employee_id,
        first_name,
        last_name,
        middle_name,
        department_id,
        site_id,
        employment_end_date,
        employment_start_date,
        employment_type,
        company_id,
        employee_code
    from {{ ref('stg_tensor__employee') }}
    where employee_code::text ~ '^[0-9]+' -- filters out non-numeric codes
),

departments as (
    select 
        department_id,
        department_code,
        department_name
    from {{ ref('stg_tensor__department') }}
),

employee_details as (
    select
        employee_id,
        country_id,
        known_as
    from {{ ref('stg_tensor__basic_detail') }} 
),

country as (
    select
        country_id,
        country_code
    from {{ ref('stg_tensor__country') }}
),

site as (
    select
        site_id,
        site_code
    from {{ ref('stg_tensor__site') }}
),

fixed_control_item as (
    select
        item_id,
        control_value
    from {{ ref('stg_tensor__fixed_control_item') }}
),

employment_details as (
    select
        employee_id,
        position_id
    from {{ ref('stg_tensor__employment_details') }}
),

positions as (
    select
        position_id,
        job_title
    from {{ ref('stg_tensor__position') }}
),

company as (
    select
        company_id,
        company_name
    from {{ ref('stg_tensor__company') }}
),

supervisor as (
    select
        employee_id,
        supervisor_employee_id
    from {{ ref('stg_tensor__supervision') }}
),

supervisor_codes as (
    select
        employee_id,
        employee_code as supervisor_code
    from employees
),

misc_text as (
    select
        employee_id,
        misc_column,
        employee_misc_text_item_id
    from {{ ref('stg_tensor__employee_misc_text') }}
    where misc_column = 10
),

misc_text_item as (
    select
        employee_misc_text_item_id,
        misc_text
    from (
        select *,
               row_number() over (partition by misc_text order by employee_misc_text_item_id) as rn
        from {{ ref('stg_tensor__employee_misc_text_item') }}
    ) sub
    where rn = 1
),

employee_locations as (
    select
        location,
        division,
        office
    from {{ ref('stg_manual__employee_locations') }}
),

final as (
    select
        em.employee_id as id,
        em.employee_code as employee_code,
        em.first_name as first_name,
        em.last_name as last_name,
        left(em.middle_name, 1) as middle_initial,
        po.job_title as job_title,
        de.department_name as department,
        mti.misc_text as location,
        left(el.division, 4) as division,
        el.office,
        sc.supervisor_code,
        ed.known_as as preferred_first_name,
        case
            when fci.control_value ilike 'Full Time' or fci.control_value = 'Part Time' then 'Employee'
            when fci.control_value ilike 'Temps -%' then 'Contingent'
            else fci.control_value
        end as assignment_type,
        si.site_code as location_code,
        case
            when em.employment_end_date < current_date then 'S'
            else 'A'
        end as active_employee,
        date(em.employment_start_date) as employment_start_date,
        date(em.employment_end_date) as employment_end_date
    from employees em
    left join departments de on em.department_id = de.department_id
    left join employee_details ed on em.employee_id = ed.employee_id
    left join site si on em.site_id = si.site_id
    left join country co on ed.country_id = co.country_id
    left join fixed_control_item fci on em.employment_type = fci.item_id
    left join employment_details emd on em.employee_id = emd.employee_id
    left join positions po on emd.position_id = po.position_id
    left join supervisor su on em.employee_id = su.employee_id
    left join supervisor_codes sc on su.supervisor_employee_id = sc.employee_id
    left join company cp on em.company_id = cp.company_id
    left join misc_text mt on em.employee_id = mt.employee_id
    left join misc_text_item mti on mt.employee_misc_text_item_id = mti.employee_misc_text_item_id
    left join employee_locations el on mti.misc_text = el.location
)

select *
from final
