with cte_category_vs_spending as (

    select {{ var('main_id')}},
    t.value['{{var('category_ref_var')}}'] as {{ var('category_ref_var') }}, sum(t.value['price']) as amount_spent
    from {{ ref('order_completed') }}, TABLE(FLATTEN(parse_json({{ var('col_ecommerce_order_completed_properties_products')}}))) t
    where {{timebound( var('col_ecommerce_order_completed_timestamp'))}} and {{ var('main_id')}} is not null
    group by {{ var('main_id')}}, {{var('category_ref_var')}}
)

select 
    {{ var('main_id')}}, 
    {{ var('category_ref_var') }} as highest_spent_category
from cte_category_vs_spending
qualify row_number() over(
    partition by {{ var('main_id')}} 
    order by amount_spent desc
) = 1