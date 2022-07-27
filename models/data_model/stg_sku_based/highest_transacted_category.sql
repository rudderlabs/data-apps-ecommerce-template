with cte_id_stitched_order_completed as 
(select distinct b.main_id as main_id, properties_products, timestamp from {{ source('ecommerce', 'order_completed') }} a left join 
ANALYTICS_DB.DATA_APPS_SIMULATED.{{var('id_stitcher_name')}} b 
on (a.user_id = b.other_id and b.other_id_type = 'user_id')),

cte_category_vs_transactions as
(select main_id,
t.value['{{var('category_ref_var')}}'] as {{ var('category_ref_var') }},
count(t.value['{{var('category_ref_var')}}']) as no_of_transactions
from cte_id_stitched_order_completed, TABLE(FLATTEN(parse_json(properties_products))) t
where timestamp >= '{{ var('start_date') }}' and timestamp <= '{{ var('end_date') }}' and main_id is not null
group by main_id, {{var('category_ref_var')}})

select b.main_id, {{ var('category_ref_var') }} as highest_transacted_category 
from cte_category_vs_transactions b
where no_of_transactions = (select max(no_of_transactions) from cte_category_vs_transactions a where a.main_id = b.main_id)