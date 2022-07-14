with cte_id_stitched_identifies as 
(select distinct b.main_id as main_id, context_device_type, timestamp from {{ source('ecommerce', 'identifies') }} a left join 
ANALYTICS_DB.DATA_APPS_SIMULATED.{{var('id_stitcher_name')}} b 
on (a.user_id = b.other_id and b.other_id_type = 'user_id') or (a.properties_email = b.other_id and b.other_id_type = 'email'))
select main_id, max(case when lower(context_device_type) like '%pc' then TRUE else FALSE end) as is_active_on_website from cte_id_stitched_identifies
where timestamp >= '{{ var('start_date') }}' and timestamp <= '{{ var('end_date') }}' and main_id is not null
group by main_id