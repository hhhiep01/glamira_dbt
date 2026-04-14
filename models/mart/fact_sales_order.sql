{{
    config(
        materialized='incremental',
        schema='mart',
        unique_key='sales_order_key',
        cluster_by=['order_timestamp'],
        on_schema_change='sync_all_columns'
    )
}}

WITH staging AS (
    SELECT
        FARM_FINGERPRINT(
            CONCAT(
                event_id,
                '-',
                COALESCE(CAST(product_id AS STRING), 'NA')
            )
        ) AS sales_order_key
        , event_id
        , event_type
        , order_id
        , customer_id
        , product_id
        , store_id
        , device_id
        , ip
        , order_timestamp
        , quantity
        , unit_price
        , (quantity * unit_price) AS line_total
        , currency
    FROM {{ ref('stg_raw_events') }}
    WHERE event_type = 'checkout_success'
),

fact AS (
    SELECT
        s.sales_order_key
      , s.event_id
      , s.order_id
      , s.product_id
      , COALESCE(c.customer_key, 0)                                              AS customer_key
      , COALESCE(p.product_key, 0)                                              AS product_key
      , CAST(FORMAT_DATE('%Y%m%d', CAST(s.order_timestamp AS DATE)) AS INT64)   AS date_key
      , COALESCE(l.location_key, 0)                                             AS location_key
      , COALESCE(d.device_key, 0)                                              AS device_key
      , COALESCE(st.store_key, 0)                                              AS store_key
      , s.quantity                                                             AS order_quantity
      , s.order_timestamp
      , s.unit_price
      , s.line_total
      , s.currency
    FROM staging s
    LEFT JOIN {{ ref('dim_customer') }} c ON CAST(s.customer_id AS STRING) = c.customer_id
    LEFT JOIN {{ ref('dim_product') }}   p ON s.product_id = p.product_id
    LEFT JOIN {{ ref('dim_location') }}   l ON s.ip = l.ip
    LEFT JOIN {{ ref('dim_device') }}     d ON s.device_id = d.device_id
    LEFT JOIN {{ ref('dim_store') }}      st ON s.store_id = st.store_id
)

SELECT
    sales_order_key
  , event_id
  , order_id
  , product_id
  , customer_key
  , product_key
  , date_key
  , location_key
  , device_key
  , store_key
  , order_quantity
  , order_timestamp
  , unit_price
  , line_total
  , currency
FROM fact
{% if is_incremental() %}
WHERE sales_order_key NOT IN (SELECT sales_order_key FROM {{ this }})
{% endif %}
