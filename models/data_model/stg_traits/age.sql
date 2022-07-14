with cte_id_stitched_identifies as 
(select distinct b.main_id as main_id, properties_birthday, timestamp from {{ source('ecommerce', 'identifies') }} a left join 
ANALYTICS_DB.DATA_APPS_SIMULATED.MATERIAL_DOMAIN_PROFILE__IDSTITCHER_DC3F9DA0_12 b 
on (a.user_id = b.other_id and b.other_id_type = 'user_id') or (a.properties_email = b.other_id and b.other_id_type = 'email'))
select distinct main_id, 
{{get_age_from_dob('properties_birthday')}} as age 
from cte_id_stitched_identifies where timestamp >= '{{ var('start_date') }}' and timestamp <= '{{ var('end_date') }}'
and properties_birthday is not null and properties_birthday!=''