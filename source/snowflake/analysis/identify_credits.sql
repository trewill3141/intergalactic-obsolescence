select 
  to_variant('credit_usage') as metric,
  date_trunc('day', "timestamp") as time_bucket,
  sum("credits_used") as total_credits_used
from 
  information_schema.credits
where 
  "timestamp" > dateadd(month, -6, current_timestamp())
group by 
  to_variant('credit_usage'),
  time_bucket
order by 
  time_bucket;