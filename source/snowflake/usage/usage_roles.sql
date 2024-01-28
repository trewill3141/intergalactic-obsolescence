-- query to review snowflake credit usage AND time warehouses are open, aggregated by warehouse, day, including job details
-- cte to calculate job details
WITH jobdetails AS
(
	SELECT  warehouse_name
	       ,date_trunc('day',start_time) AS day
	       ,COUNT(distinct job_id)       AS job_count
	       ,array_agg(distinct job_type) AS job_types
	       ,array_agg(distinct role)     AS roles
	FROM information_schema.query_history
	GROUP BY  warehouse_name
	         ,day
)
-- cte to calculate warehouse metrics
, warehousemetrics AS (
SELECT  warehouse_name
       ,date_trunc('day',qh.start_time) AS day
       ,SUM(qh.credit_used)             AS total_credit_used
       ,MIN(qh.start_time)              AS warehouse_start_time
       ,MAX(qh.end_time)                AS warehouse_end_time
       ,MIN(wdh.start_time)             AS warehouse_creation_time
       ,MAX(wdh.end_time)               AS warehouse_termination_time
FROM information_schema.query_history qh
JOIN information_schema.warehouse_metering_history wdh
ON qh.warehouse_name = wdh.warehouse_name
GROUP BY  qh.warehouse_name
         ,day ) -- final query combining results
FROM both ctes
SELECT  wm.warehouse_name
       ,wm.day
       ,jd.job_count
       ,jd.job_types
       ,jd.roles
       ,wm.total_credit_used
       ,wm.warehouse_start_time
       ,wm.warehouse_end_time
       ,wm.warehouse_creation_time
       ,wm.warehouse_termination_time
FROM warehousemetrics wm
JOIN jobdetails jd
ON wm.warehouse_name = jd.warehouse_name AND wm.day = jd.day
ORDER BY wm.warehouse_name, wm.day;