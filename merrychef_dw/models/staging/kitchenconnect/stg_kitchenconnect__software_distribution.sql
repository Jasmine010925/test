WITH software_distributions AS (
    {% set software_distributions_tables = [
        'kc_pura_vida__Software_Sends',
        'kc_tim_hortons__Software_Sends',
        'km_merrychef__Software_Sends'

    ] %}
    {% for table in software_distributions_tables %}
    SELECT jobid, deviceid, progress, status, distributionid, "distributionDate","_airbyte_extracted_at", "package_label" FROM {{ source('kitchenconnect', table) }}
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
),

final AS (
    SELECT
        jobid as job_id,
        package_label,
        deviceid as device_id,
        CAST(NULLIF(progress, '') AS INTEGER) as progress,
        status,
        CASE
            WHEN CAST(NULLIF(progress, '') AS INTEGER) IN (0, 10) THEN  'In Progress'
            WHEN CAST(NULLIF(progress, '') AS INTEGER) = 50 THEN 'In Oven'
            WHEN CAST(NULLIF(progress, '') AS INTEGER) = 100 THEN 'Completed'
            ELSE 'Unknown'
        END as revised_status,
        distributionid as distribution_id,
        "distributionDate"::DATE as distribution_date,
        DATE("_airbyte_extracted_at") as data_updated
    FROM software_distributions
)

-- Select * from Final CTE
SELECT * FROM final

