with cte_id_stitched_product_added as 
(select distinct b.main_id as main_id, properties_cart_id, timestamp from {{ source('ecommerce', 'product_added') }} a left join 
ANALYTICS_DB.DATA_APPS_SIMULATED.{{var('id_stitcher_name')}} b 
on (a.user_id = b.other_id and b.other_id_type = 'user_id'))

select distinct main_id,
datediff(day, date(max(timestamp)), date({{get_end_timestamp()}})) as days_since_last_cart_add
from cte_id_stitched_product_added
where timestamp >= '{{ var('start_date') }}' and timestamp <= '{{ var('end_date') }}' and main_id is not null
group by main_id