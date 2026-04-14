{{
    config(
        materialized='incremental',
        schema='core',
        unique_key='store_key',
        on_schema_change='sync_all_columns'
    )
}}

WITH source_data AS (
    SELECT
        store_id
        , MAX(cat_id) AS category_id
    FROM {{ ref('stg_raw_events') }}
    WHERE store_id IS NOT NULL AND store_id != ''
    GROUP BY store_id
),

numbered AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY store_id) AS store_key
        , store_id
        , category_id AS store_domain
        , 'Unknown' AS country_hint
    FROM source_data
)

SELECT
    store_key
    , store_id
    , store_domain
    , country_hint
FROM numbered
