{{
    config(
        materialized='incremental',
        database='todo-459814',
        schema='glamira_core',
        unique_key='device_key',
        on_schema_change='sync_all_columns'
    )
}}

WITH source_data AS (
    SELECT
        device_id
        , MAX(user_agent) AS user_agent
        , MAX(CASE WHEN user_agent LIKE '%Mobile%' OR user_agent LIKE '%Android%' OR user_agent LIKE '%iPhone%' THEN TRUE ELSE FALSE END) AS is_mobile
    FROM {{ ref('stg_raw_events') }}
    WHERE device_id IS NOT NULL AND device_id != ''
    GROUP BY device_id
),

numbered AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY device_id) AS device_key
        , device_id
        , user_agent
        , CASE
            WHEN user_agent LIKE '%Mobile%' OR user_agent LIKE '%Android%' THEN 'mobile'
            WHEN user_agent LIKE '%Tablet%' OR user_agent LIKE '%iPad%' THEN 'tablet'
            ELSE 'desktop'
          END AS device_type
        , CASE
            WHEN user_agent LIKE '%Chrome%' THEN 'Chrome'
            WHEN user_agent LIKE '%Firefox%' THEN 'Firefox'
            WHEN user_agent LIKE '%Safari%' THEN 'Safari'
            WHEN user_agent LIKE '%Edge%' THEN 'Edge'
            ELSE 'Unknown'
          END AS browser_name
        , CASE
            WHEN user_agent LIKE '%Windows%' THEN 'Windows'
            WHEN user_agent LIKE '%Mac OS%' THEN 'macOS'
            WHEN user_agent LIKE '%Linux%' THEN 'Linux'
            WHEN user_agent LIKE '%Android%' THEN 'Android'
            WHEN user_agent LIKE '%iOS%' THEN 'iOS'
            ELSE 'Unknown'
          END AS os_name
        , is_mobile
    FROM source_data
)

SELECT
    device_key
    , device_id
    , user_agent
    , device_type
    , browser_name
    , os_name
    , is_mobile
FROM numbered

UNION ALL

-- Default row: key = -1 means "unknown / not applicable device"
SELECT
    -1                          AS device_key
    , 'Unknown'                  AS device_id
    , 'Unknown'                  AS user_agent
    , 'Unknown'                  AS device_type
    , 'Unknown'                  AS browser_name
    , 'Unknown'                  AS os_name
    , FALSE                      AS is_mobile
