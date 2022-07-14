
  create or replace  view ANALYTICS_DB.dbt_ecommerce_features.stg_products_viewed 
  
   as (
    select * from analytics_db.data_apps_simulated.tracks where timestamp >= '2021-05-01'
  );
