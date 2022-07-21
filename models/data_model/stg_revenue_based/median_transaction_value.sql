with cte_id_stitched_order_completed as 
(select distinct b.main_id as main_id, properties_total, timestamp from {{ source('ecommerce', 'order_completed') }} a left join 
ANALYTICS_DB.DATA_APPS_SIMULATED.{{var('id_stitcher_name')}} b 
on (a.user_id = b.other_id and b.other_id_type = 'user_id')),

cte_ordered_properties_total as 
(select main_id, timestamp, properties_total,
row_number() over (partition by main_id order by properties_total) as row_id,
count(*) over(partition by main_id) as ct
from cte_id_stitched_order_completed)

select distinct main_id,
avg(properties_total) as median_transaction_value
from cte_ordered_properties_total
where row_id between ct/2.0 and ct/2.0 + 1
and timestamp >= '{{ var('start_date') }}' and timestamp <= '{{ var('end_date') }}' and main_id is not null
group by main_id