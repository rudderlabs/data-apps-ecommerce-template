with cte_id_stitched_identifies as (

    select distinct b.{{ var('main_id')}}, 
    {{ var('col_ecommerce_identifies_timestamp')}}, 
    {{ var('col_ecommerce_identifies_campaign_source')}} 
    from {{ var('tbl_ecommerce_identifies') }} a 
    left join {{ var('tbl_id_stitcher')}} b 
    on (a.{{ var('col_ecommerce_identifies_user_id')}} = b.{{ var('col_id_stitcher_other_id')}} and b.{{ var('col_id_stitcher_other_id_type')}} = 'user_id') or (a.{{ var('col_ecommerce_identifies_email')}} = b.{{ var('col_id_stitcher_other_id')}} and b.{{ var('col_id_stitcher_other_id_type')}} = 'email')

)

select 
    {{ var('main_id')}}, 
    array_agg(distinct {{ var('col_ecommerce_identifies_campaign_source')}} ) as campaign_sources 
from cte_id_stitched_identifies
where {{timebound( var('col_ecommerce_identifies_timestamp'))}} and {{ var('main_id')}} is not null
group by {{ var('main_id')}}