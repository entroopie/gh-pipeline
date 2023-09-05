{{ 
    config(materialized="incremental")
}}

SELECT 
    DATE_TRUNC(created_at, DAY) AS day, 
    type, 
    COUNT(id) AS number_of_events
FROM {{ ref('ghdata_rm_duplicates') }}
GROUP BY type
ORDER BY day, type