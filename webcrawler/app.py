import requests
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
import pandas as pd
from datetime import date  # Built-in Python library
import json  # Built-in Python library
import boto3
import re
import logging  # Built-in Python library

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def scrape_site(url, text):
    try:
        # Request the page's html script
        hdr = {'User-Agent': 'Mozilla/5.0'}
        req = Request(url,headers=hdr)
        page = urlopen(req)

        # Then parse it
        soup = BeautifulSoup(page, "html.parser")

        # Search for the text
        html_lists = soup.find_all(string=re.compile(text))

        return len(html_lists)>0
    except Exception as e:
        logger.error(f"Error scraping {url}: {e}")
        return False

def lambda_handler(event, context):
    try:
        logger.info('Start data retriving...')

        # Prepare the DynamoDB client
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('TABLE_NAME')

        # Initialize variables for pagination
        last_evaluated_key = None
        while True:
            # Perform a paginated scan
            if last_evaluated_key:
                response = table.scan(ExclusiveStartKey=last_evaluated_key)
            else:
                response = table.scan()  
            
            for item in response['Items']:
                # Extract and process attributes
                attrFIT_value = item['FIT']
                attrTEXT_value = item['TEXT']
                attrURL_value = item['URL']

                # Scrape the website
                scrape_value=scrape_site(url=attrURL_value, text=attrTEXT_value)
                
                # The FIT item is used to stop the execution of the function (if its value is FALSE), 
                # because we are looking for data different from the TEXT item.
                if not attrFIT_value:
                    scrape_value = not scrape_value
                    
                if scrape_value:
                    # Prepare the SNS client and send the message
                    sns = boto3.client('sns')
                    # creating and sending an e-mail message
                    # The address of the TopicArn is generic. Please create your own TopicArn through AWS.
                    response = sns.publish(
                        TopicArn='arn:aws:sns:eu-west-1:0123456789:MyTestTopic',
                        Message = 'Sistem found a match on the URL: ' + attrURL_value,
                        Subject = 'You have an AWS webcrawler matching'
                    )

            # Update the last evaluated key
            last_evaluated_key = response.get('LastEvaluatedKey')

            # If there's no more data to retrieve, exit the loop
            if not last_evaluated_key:
                break
        
        logger.info('Data retrieval complete!')
    except Exception as e:
        logger.error(f"Error in lambda handler: {e}")
