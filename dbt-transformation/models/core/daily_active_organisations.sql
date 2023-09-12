{{ 
    config(materialized="incremental")
}}

WITH pr_activity AS (
    SELECT
        created_at,
        org_id,
        org_login
    FROM {{ ref('ghdata_rm_duplicates') }}
    WHERE type = 'PullRequestEvent' AND org_id IS NOT NULL
)
SELECT 
    DATE_TRUNC(CAST(created_at AS DATE), DAY) AS _day,
    org_id,
    org_login,
    COUNT(1) as number_of_PullRequest
FROM pr_activity
GROUP BY 1,2,3
ORDER BY number_of_PullRequest DESC
