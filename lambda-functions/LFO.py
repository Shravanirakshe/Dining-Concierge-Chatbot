"""
LF0 Lambda Function performs -- 
1) It is linked with API Gateway and gets triggered with /chatbot post request
2) Collects user input from /chatbot REST API
3) Sends the user input to Lex 
4) Extract the output from Lex 
5) Send the Lex response back to user using /chatbot
"""

import boto3
import os

client = boto3.client('lexv2-runtime')

# Getting values from environment variables
botId = os.environ.get('BOT_ID')
botAliasId = os.environ.get('BOT_ALIAS_ID')
localeId = os.environ.get('LOCALE')
sessionId = os.environ.get('SESSION_ID')


def lambda_handler(event, context):
    
    # Sending incoming message body to LEX and getting response from LEX
    responseMsg = client.recognize_text(
        botId=botId,
        botAliasId=botAliasId,
        localeId=localeId,
        sessionId=sessionId,
        text=event["messages"][0]['unstructured']['text']
        )
        
    message = responseMsg['messages'][0]['content']
    
    return {
        'statusCode': 200,
        'messages': [{'type': 'unstructured', 'unstructured': {'text': message }}]
    }
    
    