{#
    Macro for generating surrogate keys using MD5 hash
    Used for creating unique IDs in dimension tables
#}

{% macro generate_surrogate_key(columns) %}
    {{ dbt.hash(dbt.concat(columns)) }}
{% endmacro %}
