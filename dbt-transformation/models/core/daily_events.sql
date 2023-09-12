{{ 
    config(materialized="incremental")
}}

SELECT 
    DATE_TRUNC(CAST(created_at AS DATE), DAY) AS _day, 
    type, 
    COUNT(id) AS number_of_events
FROM {{ ref('ghdata_rm_duplicates') }}
GROUP BY 2, 1
ORDER BY 2, 1