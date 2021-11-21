import os
import re
import requests
import hashlib
import json
import boto3


def lambda_handler(event, context):

    print("Printing event...")
    print(event)
    print("Printing context...")
    print(context)

    if "SQS_QUEUE" in os.environ:
        sqs_queue = os.environ["SQS_QUEUE"]
    else:
        sqs_queue = "aws_service_ingestor"


    def md5(file):
        hash_md5 = hashlib.md5()
        with open(file, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()


    def download_file(targetURL, filename):
        print("Downloading file from " + targetURL + "...\n")
        response = requests.get(targetURL, stream=True)

        with open(filename, 'wb') as f:
            f.write(response.content)


    offer_index_filename = "/tmp/offer_index.json"

    offer_index_exists = os.path.isfile(offer_index_filename)

    offer_index_url = "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/index.json"

    if offer_index_exists:
        resp = requests.head(offer_index_url)
        md5_remote = resp.headers['etag'][1:-1]
        if md5(offer_index_filename) == md5_remote:
            print("You already have the latest offer index!")
        else:
            download_file(offer_index_url, offer_index_filename)
    else:
        download_file(offer_index_url, offer_index_filename)

    with open(offer_index_filename) as json_data:
        offer_index = json.load(json_data)

    filenames = []
    urls = []

    # Create SQS client
    sqs = boto3.client('sqs')

    for offer, offer_info in offer_index['offers'].items():
        filenames.append(offer + ".csv")
        urls.append(offer_info['currentVersionUrl'])

    for url in urls:
        base_url = "https://pricing.us-east-1.amazonaws.com"

        url = url[:-5] + ".csv"

        url = base_url + url
        print(url)

        response = sqs.send_message(
            QueueUrl=sqs_queue,
            MessageBody=url
        )

        print(response)
