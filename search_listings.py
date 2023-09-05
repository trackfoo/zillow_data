import requests
import pandas as pd
import time

def get_listing_details(api_key, zillowIds, url_type):

    listing_details = []
    photos = []
    events = []
    recordCount = 0

    api_url = "https://app.scrapeak.com/v1/scrapers/zillow/property"

    for id in zillowIds:

        time.sleep(1)

        params = {"api_key": api_key, "zpid": id}
        response = requests.get(api_url, params=params)

        if response.status_code != 200:
            print(f"Error with status code {response.status_code} for id {id}")
            continue

        data = response.json()
        response_content = data.get('data', {})

        if not response_content:
            print(f"No data returned for id: {id}")
            continue

        # Grab key data then Append
        listing_details.append([id,
                                response_content.get('daysOnZillow', None),
                                response_content.get('isZillowOwned', None),
                                response_content.get('price', None),
                                response_content.get('bedrooms', None),
                                response_content.get('bathrooms', None),
                                response_content.get('homeStatus', None),
                                response_content.get('latitude', None),
                                response_content.get('longitude', None),
                                response_content.get('streetAddress', None),
                                response_content.get('zipcode', None),
                                response_content.get('city', None)])
        
        # Grab full event history for id
        for history in response_content.get('priceHistory', []):
            events.append([id,
                           history.get('event', None),
                           history.get('price', None),
                           history.get('date', None),
                           history.get('priceChangeRate', None)])

        # Grab up to 25 photos for id
        for photo in response_content.get('responsivePhotos', [])[:25]:
            photos.append([id, photo.get('url')])

        recordCount += 1
        if recordCount % 25 == 0:
            print('Records processed:', recordCount)

    # Convert lists to DataFrames
    listing_details_df = pd.DataFrame(listing_details, columns=['zillowId','daysOnZillow','isZillowOwned','listPrice','bedrooms','bathrooms','status','latitude','longitude','streetAddress','zipCode','city'])
    photos_df = pd.DataFrame(photos, columns=['zillowId','picURL'])
    events_df = pd.DataFrame(events, columns=['zillowId','event','eventPrice','eventDate','eventRateChange'])

    # Save DataFrames to CSV
    suffix = '_sold.csv' if url_type == 'sold' else '.csv'
    listing_details_df.to_csv(f'csv_files/listing_details{suffix}', index=False)
    photos_df.to_csv(f'csv_files/photos{suffix}', index=False)
    events_df.to_csv(f'csv_files/events{suffix}', index=False)
    file = f'csv_files/events{suffix}'

    return file