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
    from {{ ref('identifies')}} 
    where {{timebound( var('col_ecommerce_identifies_timestamp'))}} and {{ var('col_ecommerce_identifies_email')}} is not null and {{ var('col_ecommerce_identifies_email')}} != ''
) where rn = 1