{% macro id_stitch(TABLE_NAME, list_id_cols, timestamp_col='') %}
SELECT b.{{ var('main_id') }}, a.*
FROM
  (SELECT *
   FROM {{TABLE_NAME}} 
   {% if timestamp_col != '' %}
   WHERE {{timebound(timestamp_col)}} 
   {% endif %} ) a
LEFT JOIN {{ var('tbl_id_stitcher') }} b ON 
{% for id_col in list_id_cols %} 
        a.{{id_col}} = b.{{ var('col_id_stitcher_other_id')}} 
        {% if not loop.last %}
                OR 
        {% endif %} 
{% endfor %} 
{% endmacro %}