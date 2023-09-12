{{ 
    config(materialized="incremental")
}}

WITH push_activity AS (
    SELECT
        created_at,
        actor_id,
        actor_login
    FROM {{ ref('ghdata_rm_duplicates') }}
    WHERE type = 'PushEvent'
)
SELECT 
    DATE_TRUNC(CAST(created_at AS DATE), DAY) as _day,
    actor_id,
    actor_login,
    COUNT(1) as number_of_PushEvent
FROM push_activity
GROUP BY 1,2,3
ORDER BY number_of_PushEvent DESC
