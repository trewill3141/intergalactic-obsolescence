select 
  warehouse_name,
  date_trunc('day', start_time) as time_bucket,
  avg(elapsed_time) as avg_elapsed_time,
  avg(credits_used) as avg_credits_used
from 
  information_schema.warehouse_load_history
where 
  start_time > dateadd(month, -6, current_timestamp())
group by 
  warehouse_name,
  time_bucket
order by 
  time_bucket, avg_credits_used desc;