select 
    distinct {{ var('main_id') }},
    first_value({{ var('col_ecommerce_order_completed_properties_total') }}) over(
        partition by {{ var('main_id') }} 
        order by case when {{ var('col_ecommerce_order_completed_properties_total') }} is not null then 2 else 1 end desc, timestamp desc
    ) as last_transaction_value
from {{ ref('order_completed') }}
where {{timebound( var('col_ecommerce_order_completed_timestamp'))}} and {{ var('main_id')}} is not null