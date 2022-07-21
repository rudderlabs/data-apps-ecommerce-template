select a.main_id, (total_amt_spent/total_transactions) as avg_transaction_value 
from {{ref('total_amt_spent')}} a join {{ref('total_transactions')}} b
on a.main_id = b.main_id
group by a.main_id, a.total_amt_spent, b.total_transactions