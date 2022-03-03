"""
This Script is used to fetch Yelp data from Yelp Fusion API and upload the data to DynamoDB table
"""

def fetch_yelp_data():
    # Yelp API setup
    client_id = '<CLIENT_ID>'
    api_key = '<YELP_API_KEY>'

    # pip install yelpapi
    import pandas as pd 
    from yelpapi import YelpAPI
    yelp_api = YelpAPI(api_key)

    location = 'Manhattan, NY'
    search_limit = 50
    cuisines = ['thai', 'chinese', 'italian', 'mexican', 'american']
    offsets = [0,50,100,150,200,250,300,350,400,450,500,550,600,650,700,760,800,850,900,950]

    cus1 = []
    cus2 = []
    cus3 = []
    cus4 = []
    cus5 = []

    for off in offsets:
        cus1.append(yelp_api.search_query(term = cuisines[0],location = location, limit = search_limit, offset = off))

    for off in offsets:
        cus2.append(yelp_api.search_query(term = cuisines[1],location = location, limit = search_limit, offset = off))

    for off in offsets:
        cus3.append(yelp_api.search_query(term = cuisines[2],location = location, limit = search_limit, offset = off))

    for off in offsets:
        cus4.append(yelp_api.search_query(term = cuisines[3],location = location, limit = search_limit, offset = off))

    for off in offsets:
        cus5.append(yelp_api.search_query(term = cuisines[4],location = location, limit = search_limit, offset = off))


    cols = list(cus1[0]['businesses'][0].keys())
    df = pd.DataFrame(columns=cols)
    ct = 1

    for cus in cus1:
        for biz in cus['businesses']:
            biz['cuisine'] = cuisines[0]
            biz['r_id'] = ct
            ct+=1
            df = df.append(biz, ignore_index=True)
            
    for cus in cus2:
        for biz in cus['businesses']:
            biz['cuisine'] = cuisines[1]
            biz['r_id'] = ct
            ct+=1
            df = df.append(biz, ignore_index=True)

    for cus in cus3:
        for biz in cus['businesses']:
            biz['cuisine'] = cuisines[2]
            biz['r_id'] = ct
            ct+=1
            df = df.append(biz, ignore_index=True)

    for cus in cus4:
        for biz in cus['businesses']:
            biz['cuisine'] = cuisines[3]
            biz['r_id'] = ct
            ct+=1
            df = df.append(biz, ignore_index=True)

    for cus in cus5:
        for biz in cus['businesses']:
            biz['cuisine'] = cuisines[4]
            biz['r_id'] = ct
            ct+=1
            df = df.append(biz, ignore_index=True)

    df.to_csv('restaurants_data.csv')


def dynamoDB_data_bulk_upload():
    # Push data to dynamo db
    # Add AWS CLI to Local

    import boto3
    import datetime
    import csv

    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

    table = dynamodb.Table('yelp-restaurants')

    with open('restaurants_data.csv', encoding='utf-8', newline='') as f:
        reader=csv.reader(f)
        restaurants=list(reader)
    restaurants=restaurants[1:]


    for restaurant in restaurants:
    
        tableEntry = {
        'r_id': restaurant[18],
        'id': restaurant[1],
        'name': restaurant[3],
        'address': restaurant[13],
        'coordinates': restaurant[10],
        'review_count': restaurant[7],
        'rating': restaurant[9],
        'zip_code': restaurant[13].split("'zip_code': '")[1].split("'")[0],
        'cuisine': restaurant[17]}

        table.put_item(
            Item = {
            'insertedAtTimestamp': str(datetime.datetime.now()),
            'r_id': tableEntry['r_id'],
            'id': tableEntry['id'],
            'name': tableEntry['name'],
            'address': tableEntry['address'],
            'rating': tableEntry['rating'],
            'review_count': tableEntry['review_count'],
            'coordinates': tableEntry['coordinates'],
            'zip_code': tableEntry['zip_code'],
            'cuisine': tableEntry['cuisine']
        })


fetch_yelp_data()
dynamoDB_data_bulk_upload()