import zillowfunctions as zf
import process_request as pr
import cloud_bucket as u
import os
from google.cloud import storage

# Kick off process to grab for sale and sold data respectively
pr.process_data('for_sale')
pr.process_data('sold')
print('All data received.  Kicking off file processing jobs.')

# Join events and listing files for each group; Move reference files to archive
zf.join_files('csv_files/listing_details.csv','csv_files/rank_events.csv','csv_files/prep_data.csv')
zf.join_files('csv_files/listing_details_sold.csv','csv_files/rank_events_sold.csv','csv_files/prep_data_sold.csv')
print('All files joined.')

# Concat raw events and photos files; Move reference files to archive
zf.concatenate_and_save("csv_files/events.csv", "csv_files/events_sold.csv", "csv_files/raw_events_final.csv")
zf.concatenate_and_save("csv_files/photos.csv", "csv_files/photos_sold.csv", "csv_files/photos_final.csv")
zf.concatenate_and_save("csv_files/prep_data.csv", "csv_files/prep_data_sold.csv", "csv_files/dashboard_data_final.csv")

# Upload files to Google Cloud Storage
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'google_cloud_service_account.json'
storage_client = storage.Client()

files = ["csv_files/raw_events_final.csv","csv_files/photos_final.csv","csv_files/dashboard_data_final.csv"]

for file in files:
    if file == "csv_files/raw_events_final.csv":
        storage_file_name = 'Raw_Events'
    elif file == "csv_files/photos_final.csv":
        storage_file_name = 'Photos'
    elif file == "csv_files/dashboard_data_final.csv":
        storage_file_name = 'Home_Details'
    u.upload_to_bucket(storage_client,storage_file_name,file,'zillow_data_bucket')

print('Script complete.')


