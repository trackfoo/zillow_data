import os
from google.cloud import storage # pip install google-cloud-storage

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'google_cloud_service_account.json'

storage_client = storage.Client()

def upload_to_bucket(storage_client,blob_name, file_path, bucket_name):
    try:
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(file_path)
        return True
    except Exception as e:
        print(e)
        return False