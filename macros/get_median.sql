{%macro get_median(total_col, table_name)%}
(

select avg({{total_col}}) as median
from cte_ordered_{{total_col}}
where row_id between ct/2.0 and ct/2.0 + 1)
{%endmacro%}