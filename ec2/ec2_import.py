#!/usr/bin/env python2

import os.path
import requests
import hashlib
import MySQLdb

def md5(file):
    hash_md5 = hashlib.md5()
    with open(file, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def download_csv():
    print("Downloading CSV...")
    response = requests.get(url, stream=True)

    with open('./ec2_prices.csv', 'wb') as f:
        f.write(response.content)

url = "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/index.csv"

file_exists = os.path.isfile('./ec2_prices.csv')

if file_exists:
    resp = requests.head(url)
    md5_remote = resp.headers['etag'][1:-1]
    if md5('./ec2_prices.csv') == md5_remote:
        print("You already have the latest csv!")
    else:
        download_csv()
else:
    download_csv()

db = MySQLdb.connect(host="localhost", # The Host
                      user="pricer", # username
                      passwd="pricer123", # password
                      db="aws_pricing") # name of the data base

cursor = db.cursor()
Query = """ LOAD DATA LOCAL INFILE './ec2_prices.csv'
INTO TABLE ec2_prices
FIELDS TERMINATED BY ','
    ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 6 LINES; """

cursor.execute(Query)
db.commit()
cursor.close()
