{{
    config(
        materialized='incremental',
        schema='core',
        unique_key='product_key',
        on_schema_change='sync_all_columns'
    )
}}

WITH source_data AS (
    SELECT DISTINCT
        CAST(product_id AS INT64) AS product_id
    FROM {{ ref('stg_raw_events') }}
    WHERE product_id IS NOT NULL
),

product_enrichment AS (
    SELECT
        s.product_id
        , COALESCE(pn.product_name, 'Product ' || CAST(s.product_id AS STRING)) AS product_name
    FROM source_data s
    LEFT JOIN {{ source('glamira_raw', 'product_names_raw') }} pn
        ON s.product_id = pn.product_id
)

SELECT
    FARM_FINGERPRINT(CAST(product_id AS STRING)) AS product_key
    , product_id
    , product_name
    , CURRENT_TIMESTAMP()                         AS updated_at
FROM product_enrichment
