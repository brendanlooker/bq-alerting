# bq-alerting

Using BigQuery Information Schema to identify long running BQ queries and queries with a large queue time.

Log 'Incidents' in a BigQuery table

Trigger a Cloud Function to generate a Cloud Logging entry that can then be used to trigger an Alert to notify support.
