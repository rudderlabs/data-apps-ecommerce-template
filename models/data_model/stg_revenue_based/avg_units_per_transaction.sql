select 
    {{ var('main_id') }}, 
    avg({{ array_size ( parse_json( var('col_ecommerce_order_completed_properties_products')) )}}) as avg_units_per_transaction 
from {{ ref('stg_order_completed') }}
where {{timebound( var('col_ecommerce_order_completed_timestamp'))}} and {{ var('main_id')}} is not null
group by {{ var('main_id') }}
