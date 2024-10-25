import requests
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
import pandas as pd
from datetime import date  # Built-in Python library
import json  # Built-in Python library
import boto3
from botocore.exceptions import ClientError
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

def scrape_site_multiple(url, text_list):
    try:
        # Request the page's html script
        hdr = {'User-Agent': 'Mozilla/5.0'}
        req = Request(url, headers=hdr)
        page = urlopen(req)

        # Then parse it
        soup = BeautifulSoup(page, "html.parser")

        # Initialize results matrix
        results = []

        # Search for each text in the list
        for text in text_list:
            html_lists = soup.find_all(string=re.compile(text))
            results.append({
                'text': text,
                'found': len(html_lists) > 0
            })

        return results
    except Exception as e:
        logger.error(f"Error scraping {url}: {e}")
        return [{'text': text, 'found': False} for text in text_list]

def lambda_handler(event, context):
    try:
        logger.info('Start data retriving...')

        # Prepare the DynamoDB client
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('crawler_data')

        # Initialize variables for pagination
        last_evaluated_key = None
        while True:
            # Perform a paginated scan
            if last_evaluated_key:
                response = table.scan(ExclusiveStartKey=last_evaluated_key)
            else:
                response = table.scan()  

            # Group items by URL
            url_groups = {}
            for item in response['Items']:
                url = item['URL']
                if url not in url_groups:
                    url_groups[url] = {'texts': [], 'fits': []}
                url_groups[url]['texts'].append(item['TEXT'])
                url_groups[url]['fits'].append(item['FIT'])
            
            # Process each URL group
            for url, data in url_groups.items():
                # Scrape the website with multiple texts
                scrape_results = scrape_site_multiple(url=url, text_list=data['texts'])
                
                # Check results against FIT values
                for result, fit in zip(scrape_results, data['fits']):
                    if (fit and result['found']) or (not fit and not result['found']):
                        # Initialize the boto3 client for SES
                        ses_client = boto3.client('ses', region_name='eu-west-1')

                        # Email sender and recipient
                        sender_email = "user@test1.com"  # Replace with your verified sender email
                        recipient_email = "user@test2.com"  # Replace with the recipient's email (also verified if in sandbox)
                        
                        # Subject of the email
                        subject = "You have an AWS webcrawler matching"

                        # HTML content
                        html_body = f"""
                        <html>
                        <head></head>
                        <body>
                        <h1>You have an AWS webcrawler matching</h1>
                        <p>Please check the link below for webcrawler matching:</p>
                        <p><a href="{url}">{url}</a></p>
                        <p>Text: {result['text']}</p>
                        <p>Found: {result['found']}</p>
                        </body>
                        </html>
                        """

                        # The plain text body of the email (for email clients that do not support HTML)
                        text_body = f"""
                        Please check the link below for webcrawler matching:
                        {url}

                        Text: {result['text']}

                        Found: {result['found']}
                        """

                        # Send the email
                        try:
                            response = ses_client.send_email(
                                Source=sender_email,
                                Destination={
                                    'ToAddresses': [
                                        recipient_email,
                                    ]
                                },
                                Message={
                                    'Subject': {
                                        'Data': subject,
                                        'Charset': 'UTF-8'
                                    },
                                    'Body': {
                                        'Html': {
                                            'Data': html_body,
                                            'Charset': 'UTF-8'
                                        },
                                        'Text': {
                                            'Data': text_body,
                                            'Charset': 'UTF-8'
                                        }
                                    }
                                }
                            )
                            print("Email sent! Message ID:"),
                            print(response['MessageId'])
                        except ClientError as e:
                            print("Error sending email: ", e.response['Error']['Message'])

            # Update the last evaluated key
            last_evaluated_key = response.get('LastEvaluatedKey')

            # If there's no more data to retrieve, exit the loop
            if not last_evaluated_key:
                break
        
        logger.info('Data retrieval complete!')
    except Exception as e:
        logger.error(f"Error in lambda handler: {e}")
