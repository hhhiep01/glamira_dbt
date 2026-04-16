{{
    config(
        materialized='incremental',
        database='todo-459814',
        schema='glamira_core',
        unique_key='customer_key',
        on_schema_change='sync_all_columns'
    )
}}

WITH source_data AS (
    SELECT
        customer_id
        , MAX(CASE WHEN customer_id IS NOT NULL AND customer_id != '' THEN TRUE ELSE FALSE END) AS is_registered
    FROM {{ ref('stg_raw_events') }}
    WHERE customer_id IS NOT NULL AND customer_id != ''
    GROUP BY customer_id
),

numbered AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY customer_id) AS customer_key
        , customer_id
        , is_registered
        -- Descriptive attributes: label instead of NULL
        , CASE
            WHEN is_registered THEN customer_id || '@example.com'
            ELSE 'N/A'
          END AS customer_email
    FROM source_data
)

SELECT
    customer_key
    , customer_id
    , customer_email
    , is_registered
FROM numbered

UNION ALL

-- Default row: key = -1 means "unknown / not applicable"
SELECT
    -1                          AS customer_key
    , 'Unknown'                  AS customer_id
    , 'N/A'                      AS customer_email
    , FALSE                      AS is_registered
