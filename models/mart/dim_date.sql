{{
    config(
        materialized='table',
        schema='mart'
    )
}}

WITH date_series AS (
    SELECT
        date_day
    FROM UNNEST(
        GENERATE_DATE_ARRAY(
            DATE('2020-01-01')
            , DATE('2030-12-31')
        )
    ) AS date_day
)

SELECT
    CAST(FORMAT_DATE('%Y%m%d', date_day) AS INT64) AS date_key
    , date_day AS full_date
    , CAST(EXTRACT(DAYOFWEEK FROM date_day) AS INT64) AS day_of_week
    , CASE CAST(EXTRACT(DAYOFWEEK FROM date_day) AS INT64)
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
        WHEN 7 THEN 'Sunday'
      END AS day_name
    , CAST(EXTRACT(MONTH FROM date_day) AS INT64) AS month_number
    , FORMAT_DATE('%B', date_day) AS month_name
    , CAST(EXTRACT(QUARTER FROM date_day) AS INT64) AS quarter_number
    , CAST(EXTRACT(YEAR FROM date_day) AS INT64) AS year_number
    , CASE WHEN EXTRACT(DAYOFWEEK FROM date_day) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM date_series