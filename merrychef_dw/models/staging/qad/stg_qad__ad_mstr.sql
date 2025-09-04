WITH ad_master AS ( 
    SELECT
        *
    FROM {{ source('qad', 'qad__816_ad_mstr') }} ild
)

SELECT * 
FROM ad_master
