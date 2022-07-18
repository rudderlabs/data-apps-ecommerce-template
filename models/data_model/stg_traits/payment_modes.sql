with cte_id_stitched_identifies as 
(select distinct b.main_id as main_id, properties_payment_method, timestamp from {{ source('ecommerce', 'checkout_step_completed') }} a left join 
ANALYTICS_DB.DATA_APPS_SIMULATED.{{var('id_stitcher_name')}} b 
on (a.user_id = b.other_id and b.other_id_type = 'user_id'))
select main_id, array_agg(distinct properties_payment_method) as payment_modes 
from cte_id_stitched_identifies
where timestamp >= '{{ var('start_date') }}' and timestamp <= '{{ var('end_date') }}' and main_id is not null
group by main_id