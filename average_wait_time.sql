insert into bq_cost_optimization.bq_alerting (incident_type, project_id, job_id, message, log_ts, incident_processed)
select  'average_wait_time' as incident_type,
          project_id,
          '' as job_id,
          FORMAT('Average Wait Time Threshold Exceeded in project: %s, Usage Time: %s, Avg Wait Time: %f', project_id, STRING(usage_time),avg_wait_time_min) as message,
          current_timestamp() as log_ts,
          false as incident_processed

    from (
 
              SELECT    
                    project_id, 
                    TIMESTAMP_SECONDS(600 * DIV(UNIX_SECONDS(creation_time) + 300, 600)) AS usage_time, 
                    CAST(AVG(js.wait_ms_avg)/60000 AS NUMERIC) AS avg_wait_time_min
                  FROM `brendanlooker`.`region-EU`.INFORMATION_SCHEMA.JOBS_BY_PROJECT,
                  UNNEST(job_stages) AS js 
                  WHERE TIMESTAMP(creation_time) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) 
                  GROUP BY project_id, usage_time 
                  ORDER BY avg_wait_time_min DESC, project_id
            )
    WHERE avg_wait_time_min < 0.05
