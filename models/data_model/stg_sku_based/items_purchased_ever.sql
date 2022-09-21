select {{ var('main_id')}},
array_agg(distinct t.value['{{var('product_ref_var')}}']) as {{ var('product_ref_var') }}
from {{ ref('order_completed') }}, TABLE(FLATTEN(parse_json({{ var('col_ecommerce_order_completed_properties_products')}}))) t
where {{timebound( var('col_ecommerce_order_completed_timestamp'))}} and {{ var('main_id')}} is not null
group by {{ var('main_id')}}