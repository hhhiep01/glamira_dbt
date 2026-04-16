{{
    config(
        materialized='incremental',
        database='todo-459814',
        schema='glamira_core',
        unique_key='location_key',
        on_schema_change='sync_all_columns'
    )
}}

WITH source_data AS (
    SELECT DISTINCT
        ip
    FROM {{ ref('stg_raw_events') }}
    WHERE ip IS NOT NULL AND ip != ''
),

location_enrichment AS (
    SELECT
        s.ip
        -- Clean trailing apostrophe from some country names
        , REPLACE(loc.country_long, "'", '') AS country_name
        , REPLACE(loc.country_short, "'", '') AS country_code
        , REPLACE(loc.region, "'", '')        AS region_name
        , REPLACE(loc.city, "'", '')          AS city_name
    FROM source_data s
    LEFT JOIN {{ source('glamira_raw', 'ip_locations_raw') }} loc
        ON s.ip = loc.ip
)

SELECT
    FARM_FINGERPRINT(ip) AS location_key
    , ip
    , COALESCE(city_name, 'Unknown')        AS city_name
    , COALESCE(region_name, 'Unknown')       AS region_name
    , COALESCE(country_name, 'Unknown')     AS country_name
    , COALESCE(country_code, 'XX')          AS country_code
    , 'ip_locations_raw'                    AS source
    , CURRENT_TIMESTAMP()                   AS updated_at
FROM location_enrichment

UNION ALL

-- Default row: key = -1 means "unknown IP / not applicable"
SELECT
    -1                          AS location_key
    , 'Unknown'                  AS ip
    , 'Unknown'                  AS city_name
    , 'Unknown'                  AS region_name
    , 'Unknown'                  AS country_name
    , 'XX'                       AS country_code
    , 'system'                   AS source
    , CURRENT_TIMESTAMP()        AS updated_at
