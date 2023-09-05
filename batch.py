# Import necessary libraries
from urllib import parse
import pandas as pd
import requests
import time
import json

# Comments for Steps in the process
"""
Steps:
1- Set the listing url
2- Run the createPriceChunks if you don't have price range list
3- Set priceChunkTaskId and Get price range list 
4- And then start the scraper
"""

# Set the desired settings for Pandas display
pd.set_option("display.max_columns", None)

# Function to create price chunks for the specified listing URL
def createPriceChunks(api_key, listing_url):
    # API URL for creating price chunks
    url = "https://app.scrapeak.com/v1/scrapers/zillow/priceChunks"

    # Parameters for the API request
    querystring = {
        "api_key": api_key,
        "url": listing_url
    }

    # Make a GET request to create price chunks
    resp = requests.request("GET", url, params=querystring)

    # Return the JSON response
    return resp.json()

def wait(api_key, task_id):
    while True:
        url = "https://app.scrapeak.com/api/task/status"

        # Parameters for the API request
        querystring = {
            "api_key": api_key,
            "task_id": task_id
        }
        
        resp = requests.request("GET", url, params=querystring)
        if resp.json()["data"]["status"] == "SUCCESS":
            break
        else:
            print("Waiting for the task to finish...")
            time.sleep(30)
            continue

# Check to see if taskId already exists for URL
def check_taskId(file_name, search_column, search_value, target_column):
    try:
        # Read CSV
        df = pd.read_csv(file_name)

        # Check if the value exists in the specified column
        if search_value in df[search_column].values:
            # Get the target value from the corresponding row in the target column
            target_value = df[df[search_column] == search_value][target_column].iloc[0]
            print(f"URL and taskId found in file.")
            return target_value
        else:
            print(f"No URL / taskId found.")
            return None
    except FileNotFoundError:
        print(f"The file '{file_name}' was not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Function to get the price chunk list for a given task ID
def getPriceChunkList(api_key, task_id):
    # API URL to get the price chunk list
    url = "https://app.scrapeak.com/api/task/result"

    # Parameters for the API request
    querystring = {
        "api_key": api_key,
        "task_id": task_id
    }

    # Make a GET request to get the price chunk list
    resp = requests.request("GET", url, params=querystring)

    # Return the JSON response
    return resp.json()

# Function to fetch listings for a given price range
def get_listings(api_key, listing_url):
    # API URL to fetch listings
    url = "https://app.scrapeak.com/v1/scrapers/zillow/listing"

    # Parameters for the API request
    querystring = {
        "api_key": api_key,
        "url": listing_url
    }

    # Introduce a delay to avoid overloading the server with requests
    time.sleep(5)

    # Make a GET request to fetch the listings
    return requests.request("GET", url, params=querystring)

def startScraper(priceChunkTaskId,api_key,listing_url,url_type,columns_to_keep,new_column_names):
    houseData = []

    # Check if priceChunkTaskId is provided, then fetch price chunk list
    if priceChunkTaskId:
        priceChunkListResp = getPriceChunkList(api_key, priceChunkTaskId)
        priceChunkList = priceChunkListResp["data"]["result"]["chunks"]

        # Iterate through each price range and fetch listings
        for priceRange in priceChunkList:

            # Parse and extract searchQueryStateData from the listing URL
            searchQueryStateData = {}
            for param_key, param_value in parse.parse_qs(parse.urlparse(listing_url).query).items():
                value = param_value[0]
                if value[0] == "{":
                    value = json.loads(value)
                searchQueryStateData[param_key] = value
                    
            # Start fetching listings for the given price range and pagination
            pageNumber = 1
            current_zpids = set()
            while True:
                # Update the searchQueryStateData with the current price range and page number
                searchQueryStateData["searchQueryState"]["pagination"] =  {"currentPage":pageNumber}
                searchQueryStateData["searchQueryState"]["filterState"]["price"] = {"min":priceRange["chunkMinPrice"], "max":priceRange["chunkMaxPrice"]}
                
                # Create a new listing URL with the updated searchQueryStateData
                listing_url = listing_url.split("searchQueryState=")[0]+parse.urlencode(searchQueryStateData)
                listing_url = listing_url.replace("%27", "%22").replace("True","true").replace("False", "false").replace("None","null")
                
                # Fetch the listings using the updated listing URL
                listing_response = get_listings(api_key, listing_url)

                # Check if the response was successful and process the listings
                if listing_response.status_code == 200:
                    data = listing_response.json()["data"]
                    #with open('data_example_sold.json', 'w') as f:
                        #json.dump(data, f, indent=4)
                    if "cat1" in data:
                        cat1_data = data["cat1"]
                        if data["categoryTotals"]["cat1"]["totalResultCount"] > len(current_zpids):
                            if "searchResults" in cat1_data:
                                search_results = cat1_data["searchResults"]
                                if "listResults" in search_results:
                                    # Normalize and filter the JSON data to create a DataFrame
                                    df_listings = pd.json_normalize(search_results["listResults"])
                                    
                                    # Select only desired columns                                    
                                    existing_columns = [col for col in columns_to_keep if col in df_listings.columns]
                                    df_selected = df_listings[existing_columns]
                                    
                                    new_data = [row.to_dict() for _, row in df_selected.iterrows() if row['zpid'] not in current_zpids]
                                    houseData.extend(new_data)

                                    if 'zpid' in df_selected.columns:
                                        current_zpids.update(df_selected['zpid'].tolist())
                                    else:
                                        print("Column 'zpid' not found in df_selected!")
                                        break

                                else:
                                    print('No listings found or already processed.')
                                    break
                            else:
                                print('No listings found or already processed.')
                                break
                        else:
                            # Display the error response if fetching listings failed
                            # print(listing_response.text)
                            print("Failed to fetch listings, no data in cat1, or reached end of list.")
                            break
                    else:
                        print('No records in URL.')
                        break
                
                else:
                # Display the error response if fetching listings failed
                    print(listing_response.text)
                    print("Failed to fetch listings.")  
            
                # Increment the page number for the next iteration
                pageNumber += 1

    else:
        print('Closing script, failure.')

    houseData = pd.json_normalize(houseData)

    houseData.rename(columns=new_column_names,inplace=True)

    houseData.drop_duplicates(subset='zillowId',keep='first',inplace=True)

    # Add custom columns and create csv files
    if url_type == 'sold':
        houseData.to_csv('csv_files/house_data_sold.csv',index=False)
    elif url_type == 'for_sale':
        houseData.to_csv('csv_files/house_data.csv',index=False)
    else:
        print('Failure adding new custom columns and creating csv files.')

    houseData.reset_index(drop=True,inplace=True)
    return houseData
