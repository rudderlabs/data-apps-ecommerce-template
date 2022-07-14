
  create or replace  view ANALYTICS_DB.dbt_ecommerce_features.demographics 
  
   as (
    select main_id, 'gender' as feature_name, 
least(to_timestamp('2022-06-29'), current_timestamp())
 as timestamp, null as feature_value_numeric, gender as feature_value_string, null as feature_value_array, 'string' as feature_type from ANALYTICS_DB.dbt_ecommerce_features.gender
union
select main_id, 'age' as feature_name, 
least(to_timestamp('2022-06-29'), current_timestamp())
 as timestamp, age as feature_value_numeric, null as feature_value_string, null as feature_value_array, 'numeric' as feature_type from ANALYTICS_DB.dbt_ecommerce_features.age
union
select main_id, 'country' as feature_name, 
least(to_timestamp('2022-06-29'), current_timestamp())
 as timestamp, null as feature_value_numeric, country as feature_value_string, null as feature_value_array, 'string' as feature_type from ANALYTICS_DB.dbt_ecommerce_features.country
union
select main_id, 'state' as feature_name, 
least(to_timestamp('2022-06-29'), current_timestamp())
 as timestamp, null as feature_value_numeric, state as feature_value_string, null as feature_value_array, 'string' as feature_type from ANALYTICS_DB.dbt_ecommerce_features.state
  );
