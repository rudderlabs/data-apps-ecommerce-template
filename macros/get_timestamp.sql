{% macro get_timestamp() %}
least(to_timestamp('{{ var('end_date') }}'), current_timestamp())
{% endmacro %}