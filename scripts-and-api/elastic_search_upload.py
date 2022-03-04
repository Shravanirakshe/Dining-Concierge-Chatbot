"""
This script is used to upload restaurants data to ES
"""

# Install variables and requests_aws4auth using pip
from variables import * 
import boto3
import requests
import csv

host = '<ES_ENDPOINT>' 
#INDEX/TYPE
path = 'restaurants/Restaurant/' 
region = 'us-east-1' 
service = 'es'

ESurl = host + path

with open('restaurants_data.csv', encoding='utf-8', newline='') as f:
    reader=csv.reader(f)
    restaurants=list(reader)
restaurants=restaurants[1:]

for restaurant in restaurants:
    index_data={'restaurant_id': restaurant[18],'cuisine': restaurant[17]}

    r = requests.post(ESurl, auth=("<ES_USERNAME>", "<ES_PASSWORD"), json=index_data)