with cte_id_stitched_order_completed as 
(select distinct b.main_id as main_id, properties_total, timestamp from {{ source('ecommerce', 'order_completed') }} a left join 
ANALYTICS_DB.DATA_APPS_SIMULATED.{{var('id_stitcher_name')}} b 
on (a.user_id = b.other_id and b.other_id_type = 'user_id'))

select distinct main_id,
first_value(properties_total)
over(partition by main_id order by case when properties_total is not null then 2 else 1 end desc, timestamp desc) as last_transaction_value
from cte_id_stitched_order_completed
where timestamp >= '{{ var('start_date') }}' and timestamp <= '{{ var('end_date') }}' and main_id is not null