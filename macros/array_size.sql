{% macro array_size(column_name) %}

{% if target.type == 'redshift' %}
    get_array_length( {{column_name}} )

{% else %}
    array_size( {{column_name}} )
{% endif %} 
{% endmacro %}