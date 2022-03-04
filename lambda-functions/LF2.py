
"""
LF2 Lambda Function performs -- 
1) It pulls a message from the SQS queue which is pushed from LF1 (contains all booking details)
2) Fetch random restaurant ids from Elastic Search Index based on Cuisine
3) Extract extra details of restaurant by querying DynamoDB using the restaurant ids
4) Formats a message to send the customer over phone number and email address
5) Sends text message to the phone number included in the SQS message, using SNS 
6) Sends email to the provided email address using Twilio's sendgrid API
7) Deletes message from the SQS Queue using the Receipt Handle
"""

import boto3
from boto3.dynamodb.conditions import Attr
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from bs4 import BeautifulSoup
import requests
import json
import os
import ast

sqs = boto3.client('sqs')
sns = boto3.client('sns')
dynamodb = boto3.resource('dynamodb')

# LF2 constants
sqs_url = os.environ.get('SQS_URL')
max_poll = 10
es_endpoint = os.environ.get('ES_ENDPOINT')
es_index = os.environ.get('ES_INDEX')
dynamodb_table = os.environ.get('DYNAMODB_TABLE')
from_email = os.environ.get('FROM_EMAIL')
sendgrid_api_key = os.environ.get('SENDGRID_API_KEY')
es_username = os.environ.get('ES_USERNAME')
es_password = os.environ.get('ES_PASSWORD')
subject = 'REVEALED: Checkout these Restaurants of your Interest!!!'
number_of_suggestions = 5


def fetch_msg_from_sqs():
    sqs_response = sqs.receive_message(QueueUrl=sqs_url, MaxNumberOfMessages=max_poll)
    return sqs_response['Messages'] if 'Messages' in sqs_response.keys() else []

def delete_msg_from_sqs(receipt_handle):
    sqs.delete_message(QueueUrl=sqs_url, ReceiptHandle=receipt_handle)
    print('Message with Receipt Handle {} deleted'.format(receipt_handle))

def send_email(msgToSend, emailAddress):
    message = Mail(from_email=from_email, to_emails=emailAddress, subject=subject, html_content=msgToSend)
    sg = SendGridAPIClient(sendgrid_api_key)
    response = sg.send(message)
    print(response.body)

def send_sms(msgToSend, phoneNumber):
    print(msgToSend, phoneNumber)
    response = sns.publish(PhoneNumber='+1{}'.format(phoneNumber),Message=msgToSend, MessageStructure='string')
    print('SNS Response-> {}'.format(response))


def query_es(cuisine):

    es_query = '{}{}/_search?q={cuisine}'.format(es_endpoint, es_index, cuisine=cuisine)
    es_data = {}

    es_response = requests.get(es_query, auth=(es_username, es_password))

    data = json.loads(es_response.content.decode('utf-8'))
    es_data = data['hits']['hits']
   
    # extracting restaurant_ids from Elastic Search Service
    restaurant_ids = []
    for res in es_data:
        restaurant_ids.append(res['_source']['restaurant_id'])
      
    return restaurant_ids

def query_dynamo_db(restaurant_ids, cuisine, location, numberOfPpl, date, time):
    table = dynamodb.Table(dynamodb_table)
    
    msgToSend = '<strong>Hi there!!! <br>Checkout these top {number_of_suggestions} restaurants suggestions for {cuisine} cuisine in {location} for \
                {numberOfPpl} people, on {date} at {time} -</strong>'.format( number_of_suggestions = number_of_suggestions,
                cuisine=cuisine, location=location, numberOfPpl=numberOfPpl, date=date, time=time)
    
    ct = 1            
    for id in restaurant_ids:
        
        response = table.scan(FilterExpression=Attr('r_id').eq(id))
        item = response['Items'][0] if len(response['Items']) > 0 else None
        if response is None or item is None:
            continue
        
        msg = '<br> <br>[Restaurant ' + str(ct) + '] <br>'
        name = item['name']
        address = "".join(ast.literal_eval(item['address'])['display_address'])
                
        msg += name +', located at ' + address
        msgToSend += msg
        ct += 1
        if ct == number_of_suggestions + 1: break

    msgToSend += '<br> <br> <strong>We hope you will Enjoy the {} food at these restaurants !!</strong>'.format(cuisine)
    msgToSend += '<br> <br> <strong>Thanks, <br>The Dining Concierge Chatbot Team</strong>'

    return msgToSend

def upload_user_data(userName, textMsgToSend):

    suggestion_table = dynamodb.Table('user-suggestions')

    response = suggestion_table.scan(FilterExpression=Attr('user_id').eq(userName))
    item = response['Items'][0] if len(response['Items']) > 0 else None
    if response is None or item is None:
        suggestion_table.put_item(
        Item = {
            'user_id': userName,
            'last_suggestions': textMsgToSend
        })
    else:
        suggestion_table.update_item(
            Key={'user_id': userName},
        UpdateExpression="set last_suggestions= :textMsgToSend",
        ExpressionAttributeValues={
            ':val': textMsgToSend
        },
        ReturnValues="UPDATED_NEW")
        print("UPDATED USER INFO--")


def lambda_handler(event, context):
    
    messages = fetch_msg_from_sqs()
    
    print('Received {} messages from SQS'.format(len(messages)))
    
    for message in messages:
        msgData = json.loads(message['Body'])
        print('Message Body-> {}'.format(msgData))
        
        cuisine = msgData['cuisine']['value']['interpretedValue']
        location = msgData['location']['value']['interpretedValue']
        phoneNumber = msgData['phoneNumber']['value']['interpretedValue']
        emailAddress = msgData['emailAddress']['value']['originalValue']
        numberOfPpl = msgData['numberOfPpl']['value']['interpretedValue']
        date = msgData['date']['value']['interpretedValue']
        time = msgData['time']['value']['interpretedValue']
        userName = msgData['userName']['value']['interpretedValue']

        # Query Elastic Search with the cuisine
        restaurant_ids = query_es(cuisine)

        msgToSend = query_dynamo_db(restaurant_ids, cuisine, location, numberOfPpl, date, time)

        textMsgToSend = BeautifulSoup(msgToSend)

        # print("Sending SMS")
        # send_sms(textMsgToSend.get_text('\n'), phoneNumber)

        print("Sending Email")
        send_email(msgToSend, emailAddress)

        # Extra Credit 
        upload_user_data(userName, textMsgToSend.get_text())

        # Delete message from SQS Queue
        receipt_handle = message['ReceiptHandle']
        delete_msg_from_sqs(receipt_handle)

    return {
        'statusCode': 200,
        'body': 'Received {} messages from SQS'.format(len(messages))
    }