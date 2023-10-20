# Retrieving & Wrangling Data
import requests  # Version 2.27.1
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup  # Version 4.11.1
import pandas as pd  # Version 1.4.2

from datetime import date  # Built-in Python library
import json  # Built-in Python library

import boto3
import re
import os

# Code refactor
import logging  # Built-in Python library

def scrape_site(url, text):

    # Request the page's html script
    hdr = {'User-Agent': 'Mozilla/5.0'}
    req = Request(url,headers=hdr)
    page = urlopen(req)

    # Then parse it
    soup = BeautifulSoup(page, "html.parser")

    # Get all elements from the site that match 'text'
    html_lists = soup.find_all(string=re.compile(text))

    if len(html_lists)>0:
        return True
    else:
        return False

def lambda_handler(event, context):
    # Start logging
    logger = logging.getLogger('Start data retriving...')

    # Prepare the DynamoDB client
    dynamodb = boto3.resource("dynamodb")
    table_name = os.environ["TABLE_NAME"]
    table = dynamodb.Table(table_name)

    # Get the all data from table_name
    x = table.scan()

    # Iterate through the results and parse data
    for item in x['Items']:
        # Extract and process attributes as needed
        attrFIT_value = item['FIT']
        attrTEXT_value = item['TEXT']
        attrURL_value = item['URL']

        # Scrape HTML from given URL
        scrape_value=scrape_site(url=attrURL_value, text=attrTEXT_value)

        if not attrFIT_value:
            scrape_value = not scrape_value
            
        if scrape_value:
            # Prepare the e-mail client
            sns = boto3.client('sns')
            # creating and sending an e-mail message
            response = sns.publish(
                TopicArn='arn:aws:sns:eu-west-1:574430779371:MyTestTopic',
                Message = 'Sistem found a match on the URL: ' + attrURL_value,
                Subject = 'You have an AWS webcrawler matching'
            )
    
    logger.info('Data retrieved!')
