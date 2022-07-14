
  create or replace  view ANALYTICS_DB.dbt_ecommerce_features.is_member 
  
   as (
    select user_id, is_member from
(select user_id, 
(CASE WHEN properties_is_member = TRUE
      THEN TRUE
      ELSE FALSE
END) as is_member, row_number() over(partition by user_id order by timestamp desc) as rn from 
analytics_db.data_apps_simulated.identifies where timestamp >= '2021-05-01' and timestamp <= '2022-06-29'
and user_id is not null) where rn = 1
  );
