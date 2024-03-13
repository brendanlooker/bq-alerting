import functions_framework
from google.cloud import bigquery
import google.cloud.logging
import logging


client = google.cloud.logging.Client()
client.setup_logging()

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def incident_processing(cloud_event):

    data = cloud_event.data

    event_id = cloud_event["id"]
    event_type = cloud_event["type"]

    bucket = data["bucket"]
    name = data["name"]
    metageneration = data["metageneration"]
    timeCreated = data["timeCreated"]
    updated = data["updated"]

    print(f"Event ID: {event_id}")
    print(f"Event type: {event_type}")
    print(f"Bucket: {bucket}")
    print(f"File: {name}")
    print(f"Metageneration: {metageneration}")
    print(f"Created: {timeCreated}")
    print(f"Updated: {updated}")


    # Initialize BigQuery client
    client = bigquery.Client(project='project')

    # Define the SQL query to execute
    sql_query = """
    SELECT  incident_type, project_id, log_ts, count(*) as count
      FROM `project.bq_cost_optimization.bq_alerting` 
      WHERE incident_processed is false
      GROUP by 1,2,3
    """

    try:
        # Execute the SQL query
        query_job = client.query(sql_query)

        # Parse the query results
        results = query_job.result()
            

        for row in results:           
          # Custom log message generation
            log_message = (
                f"Incident Alert: Unprocessed incident in project: {row['project_id']} / "
                f"Threshold exceeded for incident type: {row['incident_type']} / " 
                f"Incident Count: {row['count']} / TimeStamp: {row['log_ts']}." 
            )
            logging.warning(log_message)  # Use warning or higher severity


        # SQL to update the incident status to ensure once-only processing
        update_incident_status_sql = """
        UPDATE `project.bq_cost_optimization.bq_alerting`
        SET incident_processed = true
        WHERE incident_processed is false
        """

        # Update incident table
        query_job = client.query(update_incident_status_sql)

        logging.info("Query executed and logs generated.")

        # Log success message
        logging.info("Query executed successfully.")

    except Exception as e:
        # Log error message
        logging.error(f"Error executing BigQuery query: {str(e)}")
  
    return "Incident Processing Complete!\n"

