with cte_id_stitched_checkout_step_completed as 
(select distinct b.main_id as main_id, properties_payment_method, timestamp from {{ source('ecommerce', 'checkout_step_completed') }} a left join 
ANALYTICS_DB.DATA_APPS_SIMULATED.{{var('id_stitcher_name')}} b 
on (a.user_id = b.other_id and b.other_id_type = 'user_id'))

select main_id, max(case when lower(properties_payment_method) in {{var('card_types')}} then 1 else 0 end) as has_credit_card
from cte_id_stitched_checkout_step_completed
where timestamp >= '{{ var('start_date') }}' and timestamp <= '{{ var('end_date') }}' and main_id is not null
group by main_id
