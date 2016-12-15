#!/usr/bin/env python2

import os
import requests
import hashlib
import MySQLdb
import csv

if "MARIADB_HOST" in os.environ:
    mariadb_host = os.environ["MARIADB_HOST"]
else:
    mariadb_host = "localhost"

if "MARIADB_USER" in os.environ:
    mariadb_user = os.environ["MARIADB_USER"]
else:
    mariadb_user = "pricer"

if "MARIADB_PASSWORD" in os.environ:
    mariadb_password = os.environ["MARIADB_PASSWORD"]
else:
    mariadb_password = "prices123"

if "MARIADB_DB" in os.environ:
    mariadb_db = os.environ["MARIADB_DB"]
else:
    mariadb_db = "aws_prices"

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
        "type": "VARCHAR(10)"
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
    "Max IOPS/volume": {
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
        "type": "VARCHAR(10)"
    },
    "Instance Capacity - 10xlarge": {
        "name": "InstanceCapacity10xLarge",
        "type": "VARCHAR(10)"
    },
    "Instance Capacity - 2xlarge": {
        "name": "InstanceCapacity2xLarge",
        "type": "VARCHAR(10)"
    },
    "Instance Capacity - 4xlarge": {
        "name": "InstanceCapacity4xLarge",
        "type": "VARCHAR(10)"
    },
    "Instance Capacity - 8xlarge": {
        "name": "InstanceCapacity8xLarge",
        "type": "VARCHAR(10)"
    },
    "Instance Capacity - large": {
        "name": "InstanceCapacityLarge",
        "type": "VARCHAR(10)"
    },
    "Instance Capacity - medium": {
        "name": "InstanceCapacityMedium",
        "type": "VARCHAR(10)"
    },
    "Instance Capacity - xlarge": {
        "name": "InstanceCapacityxLarge",
        "type": "VARCHAR(10)"
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
        "type": "VARCHAR(10)"
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
        "type": "VARCHAR(10)"
     }
}

def md5(file):
    hash_md5 = hashlib.md5()
    with open(file, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def download_csv(targetCSV):
    print("Downloading CSV...")
    response = requests.get(targetCSV, stream=True)

    with open('./ec2_prices.csv', 'wb') as f:
        f.write(response.content)

def parse_csv_schema():
    with open('./ec2_prices.csv', 'rb') as f:
        reader = csv.reader(f)
        for row in reader:
            if row[0] == "SKU":
                return generate_schema_from_row(row)
                break

def generate_schema_from_row(row):
    print "Generating SQL Schema from CSV..."
    schema_sql = "create table ec2_prices(\n"
    for column_title in row:
        if column_title in column_titles:
            schema_sql += column_titles[column_title]['name'] + ' ' + column_titles[column_title]['type'] + ",\n"
        else:
            schema_sql += ''.join(e for e in column_title if e.isalnum()) + " VARCHAR(200),\n"
    schema_sql = schema_sql[:-2]
    schema_sql += ");\n"
    return schema_sql

url = "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/index.csv"

file_exists = os.path.isfile('./ec2_prices.csv')

if file_exists:
    resp = requests.head(url)
    md5_remote = resp.headers['etag'][1:-1]
    if md5('./ec2_prices.csv') == md5_remote:
        print("You already have the latest csv!")
    else:
        download_csv(url)
else:
    download_csv(url)

ec2_schema = parse_csv_schema()

db = MySQLdb.connect(host=mariadb_host,
                      user=mariadb_user,
                      passwd=mariadb_password,
                      db=mariadb_db,
                      local_infile=1)

cursor = db.cursor()
Query = """ LOAD DATA LOCAL INFILE './ec2_prices.csv'
INTO TABLE ec2_prices
FIELDS TERMINATED BY ','
    ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 6 LINES; """

print "Checking to see if table ec2_prices exists"
cursor.execute("SELECT * FROM information_schema.tables WHERE table_schema = '" + mariadb_db + "' AND table_name = 'ec2_prices' LIMIT 1;")
if cursor.fetchone() is not None:
    print "Dropping existing table ec2_prices"
    cursor.execute(""" DROP TABLE ec2_prices; """)
print "Recreating table..."
cursor.execute(ec2_schema)
print "Loading csv data..."
cursor.execute(Query)
db.commit()
cursor.close()
