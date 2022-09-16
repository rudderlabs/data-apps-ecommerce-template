{% macro concat_columns(cols_list) %}
concat_ws(
 {% for col in cols_list %}
      {{col}}
       {% if not loop.last %} , {% endif %}
 {% endfor %})
{% endmacro %}