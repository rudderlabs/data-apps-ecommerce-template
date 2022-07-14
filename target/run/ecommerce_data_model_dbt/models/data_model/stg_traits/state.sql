
  create or replace  view ANALYTICS_DB.dbt_ecommerce_features.state 
  
   as (
    with cte_id_stitched_identifies as 
(select distinct b.main_id as main_id, properties_state, timestamp from analytics_db.data_apps_simulated.identifies a left join 
ANALYTICS_DB.DATA_APPS_SIMULATED.MATERIAL_DOMAIN_PROFILE__IDSTITCHER_DC3F9DA0_12 b 
on (a.user_id = b.other_id and b.other_id_type = 'user_id') or (a.properties_email = b.other_id and b.other_id_type = 'email'))
select distinct main_id, 
first_value(properties_state)
over(partition by main_id order by case when properties_state is not null and properties_state != '' then 2 else 1 end desc, timestamp desc) as state
from cte_id_stitched_identifies where timestamp >= '2021-05-01' and timestamp <= '2022-06-29' and main_id is not null
  );
