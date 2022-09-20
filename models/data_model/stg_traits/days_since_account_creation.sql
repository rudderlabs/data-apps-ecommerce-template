select 
    distinct {{ var('main_id')}}, 
    least(date(getdate()), '{{ var('end_date') }}') - 
    date(first_value({{ var('col_ecommerce_identifies_timestamp')}} ) over(
        partition by {{ var('main_id')}} 
        order by {{ var('col_ecommerce_identifies_timestamp')}} asc)
    ) as days_since_account_creation
from {{ ref('identifies')}} 
where {{timebound( var('col_ecommerce_identifies_timestamp'))}} and {{ var('main_id')}} is not null