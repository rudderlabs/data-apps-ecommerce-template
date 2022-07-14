select main_id, 'email_domain' as feature_name, 
least(to_timestamp('2022-06-29'), current_timestamp())
 as timestamp, null as feature_value_numeric, email_domain as feature_value_string, null as feature_value_array, 'string' as feature_type from ANALYTICS_DB.dbt_ecommerce_features.email_domain
union
select main_id, 'is_active_on_website' as feature_name, 
least(to_timestamp('2022-06-29'), current_timestamp())
 as timestamp, null as feature_value_numeric, is_active_on_website as feature_value_string, null as feature_value_array, 'string' as feature_type from ANALYTICS_DB.dbt_ecommerce_features.is_active_on_website
union
select main_id, 'has_mobile_app' as feature_name, 
least(to_timestamp('2022-06-29'), current_timestamp())
 as timestamp, null as feature_value_numeric, has_mobile_app as feature_value_string, null as feature_value_array, 'string' as feature_type from ANALYTICS_DB.dbt_ecommerce_features.has_mobile_app