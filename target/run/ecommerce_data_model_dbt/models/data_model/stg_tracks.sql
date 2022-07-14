
  create or replace  view ANALYTICS_DB.dbt_ecommerce_features.stg_tracks 
  
   as (
    select user_id, event_text, count(*) from analytics_db.data_apps_simulated.tracks group by 1, 2
  );
