{#
    Returns the default/unknown dimension row with key = -1.
    This row ensures INNER JOINs on fact tables are safe and fast,
    replacing NULL foreign keys with a descriptive "Unknown" record.

    Usage: UNION ALL {{ get_default_dimension_row() }}
    Should be added as the last row of every dimension model.
#}

{% macro get_default_dimension_row() %}
    -- Sentinel row: key = -1 means "unknown / not applicable"
    -- Used by fact tables via COALESCE(fk, -1) so INNER JOINs are safe
SELECT
    -1                                       AS {{ var('surrogate_key_name', 'dimension_key') }}
  , 'Unknown'                                AS label
  , 'Unknown'                                AS description
  , TRUE                                    AS is_default
  , CURRENT_TIMESTAMP()                    AS updated_at
{% endmacro %}
