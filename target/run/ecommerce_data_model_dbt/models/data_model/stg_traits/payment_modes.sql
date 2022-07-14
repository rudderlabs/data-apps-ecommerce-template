
  create or replace  view ANALYTICS_DB.dbt_ecommerce_features.payment_modes 
  
   as (
    with cte_id_stitched_identifies as 
(select distinct b.main_id as main_id, properties_payment_method from analytics_db.data_apps_simulated.checkout_step_completed a left join 
ANALYTICS_DB.DATA_APPS_SIMULATED.MATERIAL_DOMAIN_PROFILE__IDSTITCHER_DC3F9DA0_12 b 
on (a.user_id = b.other_id and b.other_id_type = 'user_id'))
select main_id, array_agg(distinct properties_payment_method) as payment_modes 
from cte_id_stitched_identifies
group by main_id
  );
