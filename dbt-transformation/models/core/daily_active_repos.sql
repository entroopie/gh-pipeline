{{ 
    config(materialized="incremental")
}}

WITH issue_activity AS (
    SELECT
        created_at,
        repo_id,
        repo_name
    FROM {{ ref('ghdata_rm_duplicates') }}
    WHERE type = 'IssuesEvent'
)
SELECT 
    DATE_TRUNC(CAST(created_at AS DATE), DAY) as _day,
    repo_id,
    repo_name,
    COUNT(1) as number_of_Issues
FROM issue_activity
GROUP BY 1,2,3
ORDER BY number_of_Issues DESC
