{% macro frame_clause() %}

{% if target.type == 'redshift' %}
    rows between unbounded preceding and unbounded following

{% else %}
    ""
{% endif %} 
{% endmacro %}