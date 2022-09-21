{% macro id_stitch(table_name, list_id_cols) %}

        select 
        b.{{ var('main_id') }},
        a.* from {{table_name}} a 
        left join {{ var('tbl_id_stitcher') }} b
        on 
        {% for id_col in list_id_cols %}
	a.{{id_col}} = b.{{ var('col_id_stitcher_other_id')}} 
	{% if not loop.last %}
	or 
	{% endif %}
        {% endfor %}
{% endmacro %}