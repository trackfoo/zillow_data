import pandas as pd
import requests
import psycopg2
import zillowsettings as s
import shutil

# Create function to get data from zillowAPI tool
def connect_database(database):
    try:
        conn = psycopg2.connect(
            host = s.hostname,
            dbname = database,
            user = s.username,
            password = s.password,
            port = s.port
        )
        cur = conn.cursor()
    except Exception as error:
        print(error)

    if cur is None:
        cur.close()
    if conn is None:
        conn.close()
    return cur,conn

def get_zillow_url(url_type):
    if url_type == 'for_sale':
        url = '' # Add for sale URL
    elif url_type == 'sold':
       url = '' # Add sold URL
    else:
        print('Error with URL.')
    return url

def get_listings(api_key, listing_url):
    url = "https://app.scrapeak.com/v1/scrapers/zillow/listing"

    querystring = {
        "api_key": api_key,
        "url":listing_url
    }

    results = requests.request("GET", url, params=querystring)

    results = results.json()

    return results

def columns_to_keep(url_type):
    if url_type == 'sold':
        columns_to_keep = [ 'zpid',
                            'unformattedPrice',
                            'statusType',
                            'hdpData.homeInfo.city']
    elif url_type == 'for_sale':
        columns_to_keep = ['zpid',
                'unformattedPrice',
                'statusType',
                'hdpData.homeInfo.city']
    else:
        print('error')
    return columns_to_keep

def new_column_names(url_type):
    if url_type == 'sold':
        new_column_names = {
                            "zpid":"zillowId",
                            "unformattedPrice":"price",
                            "statusType":"houseStatus",
                            "hdpData.homeInfo.city":"city"}
        
    elif url_type == 'for_sale':
        new_column_names = {
                "zpid":"zillowId",
                "unformattedPrice":"price",
                "statusType":"houseStatus",
                "hdpData.homeInfo.city":"city"}
    else:
        print('Columns_to_keep error.')
    return new_column_names


def rank_events(file,url_type):

    data = pd.read_csv(file)
    status = url_type

    df = pd.DataFrame(data)

    # Convert 'date' to datetime type for proper sorting
    df['eventDate'] = pd.to_datetime(df['eventDate'])

    # Sort the DataFrame first so that the ranking works correctly
    df.sort_values(['zillowId', 'event', 'eventDate'], ascending=[True, True, False], inplace=True)

    # Use the `rank` function with a custom method to rank each row within each group of 'id' and 'event'
    df['allEventRank'] = df.groupby(['zillowId', 'event']).rank(method='first', ascending=False)['eventDate']
    df['dateOrder'] = df.groupby(['zillowId']).rank(method='first', ascending=False)['eventDate']

    df_final = []

    for id in df['zillowId'].unique():
        sub_df = df[df['zillowId'] == id]

        # Initialize variables
        listing_price, listing_date, sold_price, sold_date,price_jump, count_price_change = None, None, None, None, None, None
        count_sold_before_listing = len(sub_df[(sub_df['event'] == 'Sold') & (sub_df['allEventRank'] < sub_df.loc[sub_df['event'] == 'Listed for sale', 'allEventRank'].min())])

        # First rule for listing_price and listing_date; For Sale should be latest listing is whats currently posted (can be price change), listing
        # date is orinally posted listing for sale date.
        if status != 'sold':
            if any((sub_df['event'] == 'Price change') & (sub_df['dateOrder'] == 1)):
                listing_price = sub_df.loc[(sub_df['event'] == 'Price change') & (sub_df['dateOrder'] == 1), 'eventPrice'].values[0]
                listing_date = sub_df.loc[(sub_df['event'] == 'Listed for sale') & (sub_df['allEventRank'] == 1), 'eventDate'].values[0]
                isListingSame = False
            else:
                listing_price_values = sub_df.loc[sub_df['event'] == 'Listed for sale', 'eventPrice'].values
                if listing_price_values.size > 0:
                    listing_price = listing_price_values[0]
                    listing_date = sub_df.loc[(sub_df['event'] == 'Listed for sale') & (sub_df['allEventRank'] == 1), 'eventDate'].values[0]
                else:
                    listing_price = None 
                    listing_date = None
                isListingSame = True
        else:
            if sub_df.loc[(sub_df['event'] == 'Listed for sale') & (sub_df['allEventRank'] == 1), 'eventDate'].empty:
                listing_date = None
                listing_price = None
            elif count_sold_before_listing > 1:
                listing_date = None
                listing_price = None
            else:
                listing_date = sub_df.loc[(sub_df['event'] == 'Listed for sale') & (sub_df['allEventRank'] == 1), 'eventDate'].values[0]
                listing_price = sub_df.loc[(sub_df['event'] == 'Listed for sale') & (sub_df['allEventRank'] == 1), 'eventPrice'].values[0]


        # Second rule for sold_price and sold_date; Sold, grab most recent sold event.  For sale should be blank
        if status != 'sold':
            sold_price, sold_date = None, None
        else:
            sold_price = sub_df.loc[(sub_df['event'] == 'Sold') & (sub_df['allEventRank'] == 1), 'eventPrice'].values[0]
            sold_date = sub_df.loc[(sub_df['event'] == 'Sold') & (sub_df['allEventRank'] == 1), 'eventDate'].values[0]

        # Count Price changes that happened after the Listed for Sale Date
        if status != 'sold':
            if not sub_df.loc[(sub_df['event'] == 'Listed for sale') & (sub_df['allEventRank'] == 1), 'eventDate'].empty:
                listing_date = sub_df.loc[(sub_df['event'] == 'Listed for sale') & (sub_df['allEventRank'] == 1), 'eventDate'].values[0]
            else:
                listing_date = None
            count_price_change = len(sub_df[(sub_df['event'] == 'Price change') & (sub_df['eventDate'] > pd.Timestamp(listing_date))])

            # Price jump: difference between the listing for sale price and the most recent price change price
            if listing_price and count_price_change > 0:
                most_recent_price_change = sub_df.loc[sub_df['event'] == 'Price change', 'eventPrice'].sort_values(ascending=False).values[0]
                if isListingSame == False:
                    price_jump = sub_df.loc[sub_df['event'] == 'Listed for sale', 'eventPrice'].values[0] - listing_price 
                else:
                    price_jump = None
        else:
            price_jump = sub_df.loc[(sub_df['event'] == 'Sold') & (sub_df['allEventRank'] == 1), 'eventRateChange'].values[0]
            if sub_df.loc[(sub_df['event'] == 'Listed for sale') & (sub_df['allEventRank'] == 1), 'eventDate'].empty:
                listing_date = None
                count_price_change = None
            else:
                listing_date = sub_df.loc[(sub_df['event'] == 'Listed for sale') & (sub_df['allEventRank'] == 1), 'eventDate'].values[0]
                count_price_change = len(sub_df[(sub_df['event'] == 'Price change') & (sub_df['eventDate'] > pd.Timestamp(listing_date))])      

        df_final.append([id,listing_price, listing_date, sold_price, sold_date,price_jump,count_price_change])

    df_final = pd.DataFrame(df_final,columns=['zillowId','listingPrice','listingDate','soldPrice','soldDate','priceJump','priceChanges'])

    # Save DataFrames to CSV
    suffix = '_sold.csv' if status == 'sold' else '.csv'
    df_final.to_csv(f'csv_files/rank_events{suffix}', index=False)


def concatenate_and_save(file1, file2, output_file):
    # Read the two CSV files into DataFrames
    df1 = pd.read_csv(file1)
    df2 = pd.read_csv(file2)

    # Concatenate the DataFrames
    result = pd.concat([df1, df2], ignore_index=True)

    # Save the result to a new CSV file
    result.to_csv(output_file, index=False)

    # Move old files to archive folder
    files = [file1,file2]
    for file in files:
        # Specify the source file path
        source = file
        destination = file.replace('csv_files/','csv_files/archive/')
        # Move the file
        shutil.move(source, destination)

def join_files(file1,file2,output_file):
    # Read the two CSV files into DataFrames
    df1 = pd.read_csv(file1)
    df2 = pd.read_csv(file2)

    # Merge
    result = pd.merge(df1,df2,on='zillowId',how='left')

    # Save the result to a new CSV file
    result.to_csv(output_file, index=False)

    # Move old files to archive folder
    files = [file1,file2]
    for file in files:
        # Specify the source file path
        source = file
        destination = file.replace('csv_files/','csv_files/archive/')
        # Move the file
        shutil.move(source, destination)




