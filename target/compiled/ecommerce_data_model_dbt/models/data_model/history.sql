select main_id, 'days_since_account_creation' as feature_name, 
least(to_timestamp('2022-06-29'), current_timestamp())
 as timestamp, days_since_account_creation as feature_value_numeric, null as feature_value_string, null as feature_value_array, 'numeric' as feature_type from ANALYTICS_DB.dbt_ecommerce_features.days_since_account_creation
union
select main_id, 'payment_modes' as feature_name, 
least(to_timestamp('2022-06-29'), current_timestamp())
 as timestamp, null as feature_value_numeric, null as feature_value_string, payment_modes as feature_value_array, 'array' as feature_type from ANALYTICS_DB.dbt_ecommerce_features.payment_modes
union
select main_id, 'campaign_sources' as feature_name, 
least(to_timestamp('2022-06-29'), current_timestamp())
 as timestamp, null as feature_value_numeric, null as feature_value_string, campaign_sources as feature_value_array, 'array' as feature_type from ANALYTICS_DB.dbt_ecommerce_features.campaign_sources