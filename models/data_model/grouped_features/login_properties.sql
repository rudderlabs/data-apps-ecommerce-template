select main_id, 'email_domain' as feature_name, {{get_end_timestamp()}} as end_timestamp, null as feature_value_numeric, email_domain as feature_value_string, null as feature_value_array, 'string' as feature_type from {{ref('email_domain')}}
union
select main_id, 'is_active_on_website' as feature_name, {{get_end_timestamp()}} as end_timestamp, is_active_on_website as feature_value_numeric, null as feature_value_string, null as feature_value_array, 'numeric' as feature_type from {{ref('is_active_on_website')}}
union
select main_id, 'has_mobile_app' as feature_name, {{get_end_timestamp()}} as end_timestamp, has_mobile_app as feature_value_numeric, null as feature_value_string, null as feature_value_array, 'numeric' as feature_type from {{ref('has_mobile_app')}}