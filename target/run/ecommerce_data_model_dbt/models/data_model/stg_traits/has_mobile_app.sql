
  create or replace  view ANALYTICS_DB.dbt_ecommerce_features.has_mobile_app 
  
   as (
    with cte_id_stitched_identifies as 
(select distinct b.main_id as main_id, context_device_type, timestamp from analytics_db.data_apps_simulated.identifies a left join 
ANALYTICS_DB.DATA_APPS_SIMULATED.MATERIAL_DOMAIN_PROFILE__IDSTITCHER_DC3F9DA0_12 b 
on (a.user_id = b.other_id and b.other_id_type = 'user_id') or (a.properties_email = b.other_id and b.other_id_type = 'email'))
select distinct main_id, has_mobile_app from 
(select main_id,
(CASE WHEN main_id IN(select main_id from cte_id_stitched_identifies where lower(context_device_type) in ('android','ios'))
      THEN TRUE
      ELSE FALSE
      END) as has_mobile_app, row_number() over(partition by main_id order by timestamp desc) as rn from 
cte_id_stitched_identifies where timestamp >= '2021-05-01' and timestamp <= '2022-06-29'
and context_device_type is not null and context_device_type != '' and main_id is not null) where rn = 1
  );
