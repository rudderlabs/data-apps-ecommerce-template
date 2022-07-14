with cte_id_stitched_identifies as 
(select distinct b.main_id as main_id, properties_email, timestamp from {{ source('ecommerce', 'identifies') }} a left join 
ANALYTICS_DB.DATA_APPS_SIMULATED.MATERIAL_DOMAIN_PROFILE__IDSTITCHER_DC3F9DA0_12 b 
on (a.user_id = b.other_id and b.other_id_type = 'user_id') or (a.properties_email = b.other_id and b.other_id_type = 'email'))
select distinct main_id, email_domain from 
(select main_id,{{get_domain_from_email('properties_email')}} as email_domain, row_number() over(partition by main_id order by timestamp desc) as rn from 
cte_id_stitched_identifies where timestamp >= '{{ var('start_date') }}' and timestamp <= '{{ var('end_date') }}'
and properties_email is not null and properties_email != '') where rn = 1