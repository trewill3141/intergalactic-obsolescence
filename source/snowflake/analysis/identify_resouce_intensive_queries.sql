select 
  query_text,
  date_trunc('day', start_time) as time_bucket,
  count(*) as query_count,
  avg(credits_used) as avg_credits_used
from 
  information_schema.query_history
where 
  start_time > dateadd(month, -6, current_timestamp())
group by 
  query_text,
  time_bucket
order by 
  time_bucket, avg_credits_used desc;
