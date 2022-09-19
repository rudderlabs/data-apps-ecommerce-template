with cte_category_vs_transactions as (

    select {{ var('main_id')}},
    t.value['{{var('category_ref_var')}}'] as {{ var('category_ref_var') }},
    count(t.value['{{var('category_ref_var')}}']) as no_of_transactions
    from {{ ref('order_completed') }}, TABLE(FLATTEN(parse_json({{ var('col_ecommerce_order_completed_properties_products')}}))) t
    where {{timebound( var('col_ecommerce_order_completed_timestamp'))}} and {{ var('main_id')}} is not null
    group by {{ var('main_id')}}, {{var('category_ref_var')}}

)

select {{ var('main_id')}}, {{ var('category_ref_var') }} as highest_transacted_category
from cte_category_vs_transactions
qualify row_number() over(
    partition by {{ var('main_id')}} 
    order by no_of_transactions desc
) = 1