import boto3
import requests
from requests.exceptions import HTTPError
from settings import API_KEY, BUCKET
from math import ceil
import time
import json

def main(event, context):
    try:
        print('Starting datalake silver layer')

        word = event['search']
        year = event['year']

        print('Year: {} | Search: {}'.format(year, word))

        url = 'http://www.omdbapi.com/?apikey={}&y={}&s={}'.format(API_KEY, year, word)
        response = requests.get(url=url)

        response.raise_for_status()
    except HTTPError as http_err:
        print('HTTP error occurred: {}'.format(http_err))
    except Exception as err:
        print('Other error occurred: {}'.format(err))
    else:
        result = response.json()

        if bool(result['Response']):
            data = get_data_from_all_pages(result=result, word=word, year=year)
            send_to_s3(data=data, word=word, year=year)
            print('Data have been stored to S3')
        else:
            print(result['Error'])

def get_data_from_all_pages(result, word, year):
    all_data = []
    max_per_page = 10
    pages = ceil(int(result['totalResults']) / max_per_page)

    for page in range(1, pages+1):
        url = 'http://www.omdbapi.com/?apikey={}&y={}&s={}&page={}'.format(API_KEY, year, word, str(page))
        time.sleep(5)
        resp = requests.get(url=url).json()

        for value in resp['Search']:
            all_data.append(value)

    return all_data

def send_to_s3(data, word, year):
    s3 = boto3.client('s3')
    filename = year + '/' + word + '.json'
    content = bytes(json.dumps(data).encode('utf-8'))
    s3.put_object(Bucket=BUCKET, Key=filename, Body=content)


if __name__ == '__main__':
    event = {
        'search': 'home',
        'year': '2010'
    }
    main(event=event, context=None)
