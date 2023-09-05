-- remove duplicates and subset some columns

{{ config(
    materialized="incremental",
    partition_by={
      "field": "created_at",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=['created_at', 'type']
)}}

WITH raw AS (
    SELECT id,
        type,
        actor.id AS actor_id,
        actor.login AS actor_login,
        repo.id AS repo_id,
        repo.name AS repo_name,
        created_at,
        org.id AS org_id,
        org.login AS org_login,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY created_at DESC NULLS LAST
            ) AS countInstances
    FROM {{ source("staging", "external_ghdata")}}

    {% if is_incremental() %}
    WHERE created_at > 
      (
        SELECT MAX(created_at)
        FROM {{ this }}
      )
    {% endif %}

)
SELECT id,
    type,
    actor_id,
    actor_login,
    repo_id,
    repo_name,
    created_at,
    org_id,
    org_login,
    countInstances
FROM raw
WHERE countInstances = 1


