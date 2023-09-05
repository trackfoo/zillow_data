import zillowfunctions as zf
import zillowsettings as s
import batch
import time
import csv
import search_listings as sl

def process_data(url_type):

    # Set taskId reference file location
    file_name = "csv_files/taskIds.csv"
    
    # Set URL for data scrape
    url = zf.get_zillow_url(url_type)

    # Check if URL already has associated taskId; If not, then create taskId and subsequent price chunks
    check_task_id = batch.check_taskId(file_name,'url',url,'taskId')

    if check_task_id == None:
        # No historical taskId found; Create job then wait until taskId is created
        taskInfo = batch.createPriceChunks(api_key=s.api_key, listing_url=url)
        time.sleep(10)
        batch.wait(api_key=s.api_key, task_id=taskInfo["data"]["task_id"])

        chunkId = taskInfo["data"]["task_id"]
        add_taskId = True

        print("The price chunks task has been created successfully.")

    elif check_task_id != None:
        chunkId = check_task_id
        add_taskId = False
        print("The price chunks task has been referenced.")

    else:
        print('Error recognizing taskId.')

    # Set column selections from zillow dataframe and rename
    columns_to_keep = zf.columns_to_keep(url_type)
    new_column_names = zf.new_column_names(url_type)

    # Grab zillow data
    print('Starting scraper...')
    df = batch.startScraper(api_key=s.api_key,priceChunkTaskId=chunkId,listing_url=url,url_type=url_type,columns_to_keep=columns_to_keep,new_column_names=new_column_names)
    print('Data retrieved successfully from API.')

    # Grab listing_details
    zillow_ids = df['zillowId'].tolist()
    file = sl.get_listing_details(s.api_key,zillow_ids,url_type)

    # Rank events
    zf.rank_events(file,url_type)

    # Add taskId to csv file if required
    if add_taskId == True:
        taskId_entry = [url,chunkId]
        with open(file_name, mode='a', newline='') as file:
            writer = csv.writer(file)
            
            # Write data row
            writer.writerow(taskId_entry)