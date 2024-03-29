
with cte_user_max_time as 
(select {{ var('main_id') }}, max({{ var('col_ecommerce_identifies_timestamp') }}) as recent_ts from 
{{ ref('stg_identifies') }} group by 1
)
select a.{{ var('main_id') }}, max({{get_age_from_dob( var('col_ecommerce_identifies_birthday'))}}) as age from {{ ref('stg_identifies') }} a left join 
cte_user_max_time b on 
a.{{ var('main_id') }} = b.{{ var('main_id') }}
group by 1