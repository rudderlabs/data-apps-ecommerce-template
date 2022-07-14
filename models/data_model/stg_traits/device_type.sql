with cte_id_stitched_identifies as 
(select distinct b.main_id as main_id, context_device_type, timestamp from {{ source('ecommerce', 'identifies') }} a left join 
ANALYTICS_DB.DATA_APPS_SIMULATED.MATERIAL_DOMAIN_PROFILE__IDSTITCHER_DC3F9DA0_12 b 
on (a.user_id = b.other_id and b.other_id_type = 'user_id') or (a.properties_email = b.other_id and b.other_id_type = 'email'))
select distinct main_id,
first_value(context_device_type)
over(partition by main_id order by case when context_device_type is not null and context_device_type != '' then 2 else 1 end desc, timestamp desc) as device_type
from cte_id_stitched_identifies where timestamp >= '{{ var('start_date') }}' and timestamp <= '{{ var('end_date') }}' and main_id is not null