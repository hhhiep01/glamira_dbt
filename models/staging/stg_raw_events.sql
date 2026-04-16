{{
    config(
        materialized='view',
        database='todo-459814',
        schema='glamira_staging',
        cluster_by=['order_timestamp'],
        on_schema_change='sync_all_columns'
    )
}}

WITH source_data AS (
    SELECT
        _id                                                AS event_id
      , collection                                        AS event_type
      , device_id
      , ip
      , order_id
      , store_id
      , user_id_db                                        AS customer_id
      , user_agent
      , resolution
      , current_url
      , cat_id
      , collect_id
      , local_time
      , utm_source
      , utm_medium
      , key_search
      , referrer_url                                      AS referrer
      , is_paypal
      , viewing_product_id
      , time_stamp
      , TIMESTAMP_SECONDS(time_stamp)                     AS order_timestamp
      , cart_products
    FROM {{ source('glamira_raw', 'raw_events') }}
),

unnested AS (
    SELECT
        s.event_id
      , s.event_type
      , s.device_id
      , s.ip
      , s.order_id
      , s.store_id
      , s.customer_id
      , s.user_agent
      , s.resolution
      , s.current_url
      , s.cat_id
      , s.collect_id
      , s.local_time
      , s.utm_source
      , s.utm_medium
      , s.key_search
      , s.referrer
      , s.is_paypal
      , s.viewing_product_id
      , s.order_timestamp
      , CAST(JSON_VALUE(cart_item, '$.product_id') AS INT64)                                                       AS product_id
      {# Price format: "1.395,00" / "7860,00" / "1'72400" / null — bỏ thousand sep (' .), thay , → . #}
      , CASE
          WHEN JSON_VALUE(cart_item, '$.price') IS NULL OR JSON_VALUE(cart_item, '$.price') = ''
          THEN NULL
          ELSE SAFE_CAST(
            REPLACE(
              REPLACE(
                REPLACE(JSON_VALUE(cart_item, '$.price'), "'", ''),
                '.',
                ''
              ),
              ',',
              '.'
            ) AS FLOAT64
          )
        END                                                                                                       AS unit_price
      , JSON_VALUE(cart_item, '$.currency')                                                                       AS currency
      , CAST(JSON_VALUE(cart_item, '$.amount') AS INT64)                                                          AS quantity
      {# Deduplicate: giữ row đầu tiên khi cùng event_id + product_id #}
      , ROW_NUMBER() OVER (
            PARTITION BY s.event_id, CAST(JSON_VALUE(cart_item, '$.product_id') AS INT64)
            ORDER BY s.order_timestamp
        )                                                                                                        AS rn
    FROM source_data s
    LEFT JOIN UNNEST(JSON_QUERY_ARRAY(s.cart_products)) AS cart_item
)

SELECT
    event_id
  , event_type
  , device_id
  , ip
  , order_id
  , customer_id
  , product_id
  , store_id
  , user_agent
  , resolution
  , current_url
  , cat_id
  , collect_id
  , local_time
  , utm_source
  , utm_medium
  , key_search
  , referrer
  , is_paypal
  , viewing_product_id
  , order_timestamp
  , unit_price
  , currency
  , quantity
FROM unnested
WHERE rn = 1
{% if is_incremental() %}
  AND event_id NOT IN (SELECT event_id FROM {{ this }})
{% endif %}
