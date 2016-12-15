#!/usr/bin/env python2

import os.path
import requests
import hashlib
import MySQLdb

column_titles = {
    "SKU": {
        "name": "SKU",
        "type": "VARCHAR(17)"
    },
    "OfferTermCode": {
        "name": "OfferTermCode",
        "type": "VARCHAR(10)"
    },
    "RateCode": {
        "name": "RateCode",
        "type": "VARCHAR(38)"
    },
    "TermType": {
        "name": "TermType",
        "type": "VARCHAR(16)"
    },
    "PriceDescription": {
        "name": "PriceDescription",
        "type": "VARCHAR(200)"
    },
    "EffectiveDate": {
        "name": "EffectiveDate",
        "type": "DATE"
    },
    "StartingRange": {
        "name": "StartingRange",
        "type": "VARCHAR(16)"
    },
    "EndingRange": {
        "name": "EndingRange",
        "type": "VARCHAR(16)"
    },
    "Unit": {
        "name": "Unit",
        "type": "VARCHAR(10)"
    },
    "PricePerUnit": {
        "name": "PricePerUnit",
        "type": "DOUBLE"
    },
    "Currency": {
        "name": "Currency",
        "type": "VARCHAR(3)"
    },
    "LeaseContractLength": {
        "name": "LeaseContractLength",
        "type": "VARCHAR(50)"
    },
    "PurchaseOption": {
        "name": "PurchaseOption",
        "type": "VARCHAR(50)"
    },
    "OfferingClass": {
        "name": "OfferingClass",
        "type": "VARCHAR(50)"
    },
    "Product Family": {
        "name": "ProductFamily",
        "type": "VARCHAR(50)"
    },
    "serviceCode": {
        "name": "ServiceCode",
        "type": "VARCHAR(50)"
    },
    "Location": {
        "name": "Location",
        "type": "VARCHAR(50)"
    },
    "Location Type": {
        "name": "LocationType",
        "type": "VARCHAR(50)"
    },
    "Instance Type": {
        "name": "InstanceType",
        "type": "VARCHAR(50)"
    },
    "Current Generation": {
        "name": "CurrentGeneration",
        "type": "VARCHAR(10)"
    },
    "Instance Family": {
        "name": "InstanceFamily",
        "type": "VARCHAR(50)"
    },
    "vCPU": {
        "name": "vCPU",
        "type": "SMALLINT"
    },
    "Physical Processor": {
        "name": "PhysicalProcessor",
        "type": "VARCHAR(50)"
    },
    "Clock Speed": {
        "name": "ClockSpeed",
        "type": "VARCHAR(50)"
    },
    "Memory": {
        "name": "Memory",
        "type": "VARCHAR(50)"
    },
    "Storage": {
        "name": "Storage",
        "type": "VARCHAR(50)"
    },
    "Network Performance": {
        "name": "NetworkPerformance",
        "type": "VARCHAR(50)"
    },
    "Processor Architecture": {
        "name": "ProcessorArchitecture",
        "type": "VARCHAR(20)"
    },
    "Storage Media": {
        "name": "StorageMedia",
        "type": "VARCHAR(15)"
    },
    "Volume Type": {
        "name": "VolumeType",
        "type": "VARCHAR(30)"
    },
    "Max Volume Size": {
        "name": "MaxVolumeSize",
        "type": "VARCHAR(10)"
    },
    "Max IOPS/Volume": {
        "name": "MaxIOPSVolume",
        "type": "VARCHAR(40)"
    },
    "Max IOPS Burst Performance": {
        "name": "MaxIOPSBurstPerformance",
        "type": "VARCHAR(40)"
    },
    "Max throughput/volume": {
        "name": "MaxThroughputPerVolume",
        "type": "VARCHAR(30)"
    },
    "Provisioned": {
        "name": "Provisioned",
        "type": "VARCHAR(10)"
    },
    "Tenancy": {
        "name": "Tenancy",
        "type": "VARCHAR(20)"
    },
    "EBS Optimized": {
        "name": "EBSOptimized",
        "type": "VARCHAR(10)"
    },
    "Operating System": {
        "name": "OS",
        "type": "VARCHAR(15)"
    },
    "License Model": {
        "name": "LicenseModel",
        "type": "VARCHAR(50)"
    },
    "Group": {
        "name": "AWSGroup",
        "type": "VARCHAR(50)"
    },
    "Group Description": {
        "name": "AWSGroupDescription",
        "type": "VARCHAR(200)"
    },
    "Transfer Type": {
        "name": "TransferType",
        "type": "VARCHAR(50)"
    },
    "From Location": {
        "name": "FromLocation",
        "type": "VARCHAR(50)"
    },
    "From Location Type": {
        "name": "FromLocationType",
        "type": "VARCHAR(50)"
    },
    "To Location": {
        "name": "ToLocation",
        "type": "VARCHAR(50)"
    },
    "To Location Type": {
        "name": "ToLocationType",
        "type": "VARCHAR(50)"
    },
    "usageType": {
        "name": "UsageType",
        "type": "VARCHAR(50)"
    },
    "operation": {
        "name": "Operation",
        "type": "VARCHAR(50)"
    },
    "Comments": {
        "name": "Comments",
        "type": "VARCHAR(200)"
    },
    "Dedicated EBS Throughput": {
        "name": "DedicatedEBSThroughput",
        "type": "VARCHAR(30)"
    },
    "Enhanced Networking Supported": {
        "name": "EnhancedNetworkingSupported",
        "type": "VARCHAR(10)"
    },
    "GPU": {
        "name": "GPU",
        "type": "SMALLINT"
    },
    "Instance Capacity - 10xlarge": {
        "name": "InstanceCapacity10xLarge",
        "type": "SMALLINT"
    },
    "Instance Capacity - 2xlarge": {
        "name": "InstanceCapacity2xLarge",
        "type": "SMALLINT"
    },
    "Instance Capacity - 4xlarge": {
        "name": "InstanceCapacity4xLarge",
        "type": "SMALLINT"
    },
    "Instance Capacity - 8xlarge": {
        "name": "InstanceCapacity8xLarge",
        "type": "SMALLINT"
    },
    "Instance Capacity - large": {
        "name": "InstanceCapacityLarge",
        "type": "SMALLINT"
    },
    "Instance Capacity - medium": {
        "name": "InstanceCapacityMedium",
        "type": "SMALLINT"
    },
    "Instance Capacity - xlarge": {
        "name": "InstanceCapacityxLarge",
        "type": "SMALLINT"
    },
    "Intel AVX Available": {
        "name": "IntelAVXAvailable",
        "type": "VARCHAR(10)"
    },
    "Intel AVX2 Available": {
        "name": "IntelAVX2Available",
        "type": "VARCHAR(10)"
    },
    "Intel Turbo Available": {
        "name": "IntelTurboAvailable",
        "type": "VARCHAR(10)"
    },
    "Physical Cores": {
        "name": "PhysicalCores",
        "type": "SMALLINT"
    },
    "Pre Installed S/W": {
        "name": "PreInstalledSW",
        "type": "VARCHAR(50)"
     },
    "Processor Features": {
        "name": "ProcessorFeatures",
        "type": "VARCHAR(50)"
     },
    "Sockets": {
        "name": "Sockets",
        "type": "SMALLINT"
     }
}

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
