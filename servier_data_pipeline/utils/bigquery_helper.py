# utils/bigquery_helper.py
from google.cloud import bigquery


def run_query(query, project_id):
    """Run a BigQuery SQL query and return the result as a DataFrame."""
    client = bigquery.Client(project=project_id)
    query_job = client.query(query)
    results = query_job.result()

    return results.to_dataframe()
