with cte_id_stitched_identifies as (

    select distinct b.{{ var('main_id')}}, 
    {{ var('col_ecommerce_identifies_email')}}, 
    {{ var('col_ecommerce_identifies_timestamp')}} 
    from {{ var('tbl_ecommerce_identifies') }} a 
    left join {{ var('tbl_id_stitcher')}} b 
    on (a.{{ var('col_ecommerce_identifies_user_id')}} = b.{{ var('col_id_stitcher_other_id')}} and b.{{ var('col_id_stitcher_other_id_type')}} = 'user_id') or (a.{{ var('col_ecommerce_identifies_email')}} = b.{{ var('col_id_stitcher_other_id')}} and b.{{ var('col_id_stitcher_other_id_type')}} = 'email')

)

select 
    distinct {{ var('main_id')}}, 
    email_domain 
from (
    select 
        {{ var('main_id')}},
        {{get_domain_from_email( var('col_ecommerce_identifies_email') )}} as email_domain, 
        row_number() over(
            partition by {{ var('main_id')}} 
            order by {{ var('col_ecommerce_identifies_timestamp')}}  desc
        ) as rn 
    from cte_id_stitched_identifies 
    where {{timebound( var('col_ecommerce_identifies_timestamp'))}} and {{ var('col_ecommerce_identifies_email')}} is not null and {{ var('col_ecommerce_identifies_email')}} != ''
) where rn = 1