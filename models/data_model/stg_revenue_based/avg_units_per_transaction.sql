with cte_id_stitched_order_completed as 
(select distinct b.main_id as main_id, properties_products, properties_order_id, timestamp from {{ source('ecommerce', 'order_completed') }} a left join 
ANALYTICS_DB.DATA_APPS_SIMULATED.{{var('id_stitcher_name')}} b 
on (a.user_id = b.other_id and b.other_id_type = 'user_id'))

select main_id, (sum(array_size(parse_json(properties_products)))/count(*)) as avg_units_per_transaction 
from cte_id_stitched_order_completed
where timestamp >= '{{ var('start_date') }}' and timestamp <= '{{ var('end_date') }}' and main_id is not null
group by main_id