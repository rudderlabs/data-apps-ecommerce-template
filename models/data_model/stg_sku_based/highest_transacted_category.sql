with cte_id_stitched_order_completed as (

    select distinct b.{{ var('main_id')}}, 
    {{ var('col_ecommerce_order_completed_properties_products')}}, 
    {{ var('col_ecommerce_order_completed_timestamp')}} 
    from {{ var('tbl_ecommerce_order_completed')}} a 
    left join {{ var('tbl_id_stitcher')}} b 
    on (a.{{ var('col_ecommerce_order_completed_user_id')}} = b.{{ var('col_id_stitcher_other_id')}} and b.{{ var('col_id_stitcher_other_id_type')}} = 'user_id')

), cte_category_vs_transactions as (

    select {{ var('main_id')}},
    t.value['{{var('category_ref_var')}}'] as {{ var('category_ref_var') }},
    count(t.value['{{var('category_ref_var')}}']) as no_of_transactions
    from cte_id_stitched_order_completed, TABLE(FLATTEN(parse_json({{ var('col_ecommerce_order_completed_properties_products')}}))) t
    where {{timebound( var('col_ecommerce_order_completed_timestamp'))}} and {{ var('main_id')}} is not null
    group by {{ var('main_id')}}, {{var('category_ref_var')}}

)

select {{ var('main_id')}}, {{ var('category_ref_var') }} as highest_transacted_category
from cte_category_vs_transactions
qualify row_number() over(
    partition by {{ var('main_id')}} 
    order by no_of_transactions desc
) = 1