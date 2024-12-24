import functions_framework
from google.cloud import bigquery, storage
import pandas as pd

@functions_framework.cloud_event
def transform_data(cloud_event):
    """
    Transform data from Cloud Storage to BigQuery
    """
    # Initialize clients
    storage_client = storage.Client()
    bigquery_client = bigquery.Client()
    
    # Reference your bucket and latest file
    bucket = storage_client.bucket('my-dummy-data')
    blobs = list(bucket.list_blobs())
    latest_blob = max(blobs, key=lambda x: x.updated)
    
    # Read data
    df = pd.read_csv(latest_blob.download_as_string())
    
    # Simple transformation
    df['processed_at'] = pd.Timestamp.now()
    
    # Load to BigQuery
    table_id = 'my-data-pipeline-445609.my_data_pipeline.transformed_data'
    job_config = bigquery.LoadJobConfig(
        write_disposition='WRITE_APPEND'
    )
    
    job = bigquery_client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )
    job.result()

    return 'Transformation complete', 200