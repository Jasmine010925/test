WITH ad_master AS ( 
    SELECT
        *
    FROM {{ source('qad', 'qad__816_cinvoice') }} ild
)

SELECT * 
FROM ad_master
