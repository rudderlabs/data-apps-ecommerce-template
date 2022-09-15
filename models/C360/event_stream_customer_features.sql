{{ config( materialized = 'incremental', 
unique_key = 'row_id' ) }} 

{% set numeric_features = dbt_utils.get_column_values(table=ref('event_stream_feature_table'), column='feature_name', where='feature_type=\'numeric\'') %}
{% set string_features = dbt_utils.get_column_values(table=ref('event_stream_feature_table'), column='feature_name', where='feature_type=\'string\'') %}
{% set array_features = dbt_utils.get_column_values(table=ref('event_stream_feature_table'), column='feature_name', where='feature_type=\'array\'') %}

select
    {{ var('main_id')}}, 
    timestamp,
    concat_ws({{ var('main_id')}}, to_char(timestamp)) as row_id,
    {% for feature_name in numeric_features %}
    max(case when feature_name='{{feature_name}}' then feature_value_numeric  
                  end) as {{feature_name}},
    {% endfor %} 
     {% for feature_name in string_features%}
    max(case when feature_name='{{feature_name}}' then feature_value_string  
                  end) as {{feature_name}},
    {% endfor %}    
    {% for feature_name in array_features%}
    max(case when feature_name='{{feature_name}}' then array_to_string(feature_value_array,',') 
                  end) as {{feature_name}}
                  {%- if not loop.last %},{% endif -%}
    {% endfor %}   
from {{ref('event_stream_feature_table')}}
where timestamp = {{get_end_timestamp()}}
group by {{ var('main_id')}}, timestamp, concat_ws({{ var('main_id')}}, to_char(timestamp))

