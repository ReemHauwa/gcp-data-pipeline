import functions_framework
import pandas as pd
from google.cloud import storage

@functions_framework.http
def ingest_data(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
    """
    # Initialize the client
    client = storage.Client()
    bucket = client.bucket('my-dummy-data')
    
    # Generate or get your data here
    df = pd.DataFrame({
        'timestamp': pd.date_range(start='now', periods=100),
        'value': range(100)
    })
    
    # Save to Cloud Storage
    blob = bucket.blob(f'data_{pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")}.csv')
    blob.upload_from_string(df.to_csv(index=False), content_type='text/csv')
    
    return 'Data ingestion complete', 200