import pytest
from functions.ingestion.main import ingest_data
from functions.transformation.main import transform_data

def test_ingest_data():
    # Mock request object
    class MockRequest:
        def get_json(self):
            return {"data": "test"}
    
    request = MockRequest()
    response = ingest_data(request)
    assert response[1] == 200  # Check status code

def test_transform_data():
    # Mock cloud event
    event = {
        "bucket": "test-bucket",
        "name": "test-file.csv"
    }
    
    response = transform_data(event)
    assert response[1] == 200  # Check status code