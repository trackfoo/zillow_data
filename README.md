# zillow_data
zillow_data retreives single family home data for listings via Scrapeak infrastructure, and posts files to Google Cloud Storage.  It seperately checks **for sale** and **sold** homes based on zillow search URLs, and pulls marquee home details including: Bedrooms; Bathrooms; Geo locations; Historical event details; and photos.  Additional chat bot and data visualization tools are available.

## Why
 I wanted to build a process which automatically gathered, stored, displayed and analyzed home sales / home availability data.  This would involve building and scheduling python scripts, storing data on cloud resources (Google Cloud Storage), utilizing AI tools (OpenAI API) as well as data vizualization programs (Looker Studio).    

## How It Works
### _Zillow Data Pull_
1. From Zillow.com, searches are built via desired filter criteria. Once a search filter is complete, the URL is referenced to pull all recognized home listings via [Scrapeak](https://docs.scrapeak.com/zillow-scraper/overview) APIs ([Price Chunking API](https://docs.scrapeak.com/zillow-scraper/endpoints/pricechunking) is leveraged for larger sized listings).

> [!NOTE]
> A Scrapeak price plan is required for larger data pulls

2. Once each zillowId is pulled from the Scrapeak [listing API](https://docs.scrapeak.com/zillow-scraper/endpoints/listing), zillowIds are then used with the [property API](https://docs.scrapeak.com/zillow-scraper/endpoints/propertydetails) to:
   * Gather home details per zillowId.  This includes (among other data points): Bedrooms; Bathrooms; Geo location; Days on Zillow; Status; ZillowId.
   * Store event status logs for every zillowId.  This includes statuses such as: Sold; Listing for sale; Listing removed; Price change; Contingent.
   * Store up to 25 photo URLs per zillowId.
3. Data is stored in Google Cloud Bucket csv files (along with all non-aggregated files locally).  This requires the creation of a Google Cloud project, bucket and service account credentials (stored in google_cloud_service_account.json).  Three seperate files are created: **Home_Details**, **Raw_Events**, and **Photos**.
   * The Home_Details, Raw_Events, and Photos files have combined data from the seperate **for sale** and **sold** listings.  

### _Chat Bot_
1. Utilizing PandasAI and Streamlit infrastructure, a chat bot was developed in order to communicate with the csv file.

## Getting Started

### _Initial Set-up & Data Pull_
1. Create a Google Cloud Project, Bucket and Service Account (Google Cloud Storage).  Service Account information should be stored in **google_cloud_service_account.json**.
2. Create a [Scrapeak API key](https://docs.scrapeak.com/zillow-scraper/authentication).  (Add to **zillowsettings.api_key**)
3. Create an [OpenAI API key](https://platform.openai.com/account/api-keys). (Add to **chatbot.py**)
4. From [zillow.com](https://zillow.com), create both a **For Sale** and **Sold** URL from a desired set of filters.  Paste the URLs into the **zillowfunctions.get_zillow_url** function variables respectively.
5. Kick off the **main.py** script
   * Once complete, csv files will be posted locally within csv_files/ directory as well as Google Cloud buckets.

### _Data Visualization_
1. Create [Looker Studio](https://lookerstudio.google.com/u/0/navigation/reporting) dashboard.
2. Add all Google Cloud Storage files as data sources.
### _Chat Bot_
1. Initiate the chat bot (locally).
```
streamlit run chatbot.py
```
2. Upload the _csv_files/dashboard_data_final.csv_ file to leverage the chat bot and ask questions about your data / document.


## Requirements
* Scrapeak API Key
* OpenAI API Key
* Google Cloud Project, Bucket and Service Account Credentials
* **Libraries:** pandas, requests, time, os, google.cloud, urllib, streamlit, shutil, pandasai, dotenv


## Roadmap / Feature Improvements
* **Batched API requests:**  Scrapeak is looking to add the feature to their APIs.  This should improve performance by limiting API requests and subsequently stay within price plan limitations, especially when a large set of listings are created (1,000+).
* **Implement Cloud Scheduler:** Currently the jobs are run locally.  Will want to migrate so auto refreshes are reliably executed and completed.
* **Tableau Dashboard:**  Currently the data is displayed within a Looker Studio Dashboard.  Will want to load data and visualize within Tableau as well.
* **Improve zillowId failure handling:**  If there's a failure in grabing details for a specific zillowId, there's no mechanism in re-attempting the data grab.

## Data Dictionary

### Dashboard_data_final

File contains marquee listing information, as well as rolled up information from raw events.

| Data Header | Description |
| --------------| ------------- |
| zillowId | Unique Zillow id for listing |
| daysOnZillow | How long the listing has been / was listed |
| isZillowOwned | If listing is owned by Zillow |
| listPrice | *For Sale:* Current list price; *Sold:* Listing sold price. |
| bedrooms | # of bedrooms |
| bathrooms | # of bathrooms |
| status | Current state of listing (for sale, sold) |
| latitude | Geo location |
| longitude | Geo location |
| streetAddress | Street address of listing |
| zipCode | Zip code of listing |
| city | City of listing |
| listingPrice | Most recent  |
| listingDate | Date the listing was posted |
| soldPrice | Sold price of listing |
| soldDate | Sold date of listing |
| priceJump | Price change value compared to previous listing price |
| priceChanges | # of 'Price change' events since most recent 'Listing for sale' event |

### Photos_final

File contains up to 25 photos per zillowId.

| Data Header | Description |
| --------------| ------------- |
| zillowId | Unique Zillow id for listing |
| url | url for listing photo |

### Raw_events_final

File contains all historical events per zillowId.

| Data Header | Description |
| --------------| ------------- |
| zillowId | Unique Zillow id for listing |
| event | Event which occurred (Listing for sale, Listing removed, Sold, Price Change, etc.) |
| eventDate | Date which event occurred. |
| eventPrice | Price of given event. |


## Version History

* 0.1 [09/01/2023]
  * Initial release
* 0.2 [09/05/2023]
  * Implemented Streamlit chat bot
  * Updated documentation accordingly 
