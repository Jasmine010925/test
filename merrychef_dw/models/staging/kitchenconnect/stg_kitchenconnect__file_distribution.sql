WITH send_distributions AS (
    {% set send_distributions_tables = [
        'kc_pura_vida__File_Sends',
        'kc_tim_hortons__File_Sends',
        'km_merrychef__File_Sends'
    ] %}
    {% for table in send_distributions_tables %}
    SELECT jobid, deviceid, progress, status, distributionid, "distributionDate", "_airbyte_extracted_at", "package_label" FROM {{ source('kitchenconnect', table) }}
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
        distributionid as distribution_id,
        "distributionDate"::DATE as distribution_date, 
        DATE("_airbyte_extracted_at") as data_updated,
        CASE
            WHEN CAST(NULLIF(progress, '') AS INTEGER) IN (0, 10) THEN 'In Progress'
            WHEN CAST(NULLIF(progress, '') AS INTEGER) = 50 THEN 'In Oven'
            WHEN CAST(NULLIF(progress, '') AS INTEGER) = 100 THEN 'Completed'
            ELSE 'Unknown'
        END as revised_status
    FROM send_distributions
)

-- Select * from Final CTE
SELECT * FROM final
