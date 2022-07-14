
  create or replace  view ANALYTICS_DB.dbt_ecommerce_features.stg_user_traits 
  
   as (
    select * from ANALYTICS_DB.dbt_ecommerce_features.demographics
union
select * from ANALYTICS_DB.dbt_ecommerce_features.login_properties
union
select * from ANALYTICS_DB.dbt_ecommerce_features.history
  );
