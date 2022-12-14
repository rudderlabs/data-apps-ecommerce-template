{% macro timestamp_call(column_name) %}
{% if target.type == 'redshift' %}
    "{{column_name}}"
{% else %}
    {{column_name}}
{% endif %} 
{% endmacro %}