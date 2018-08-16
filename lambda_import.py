import os
import requests
import hashlib
import pymysql.cursors
import csv
import json


def lambda_handler(event, context):

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
            "type": "VARCHAR(200)"
        },
        "EndingRange": {
            "name": "EndingRange",
            "type": "VARCHAR(200)"
        },
        "Unit": {
            "name": "Unit",
            "type": "VARCHAR(50)"
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
            "type": "VARCHAR(200)"
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
            "type": "VARCHAR(100)"
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
            "type": "VARCHAR(200)"
        },
        "Group Description": {
            "name": "AWSGroupDescription",
            "type": "VARCHAR(500)"
        },
        "Transfer Type": {
            "name": "TransferType",
            "type": "VARCHAR(200)"
        },
        "From Location": {
            "name": "FromLocation",
            "type": "VARCHAR(200)"
        },
        "From Location Type": {
            "name": "FromLocationType",
            "type": "VARCHAR(50)"
        },
        "To Location": {
            "name": "ToLocation",
            "type": "VARCHAR(200)"
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

    def download_file(targetURL, filename):
        print("Downloading file from " + targetURL + "...\n")
        response = requests.get(targetURL, stream=True)

        with open(filename, 'wb') as f:
            f.write(response.content)

    def parse_csv_schema(filename, table_name):
        with open(filename, 'rb') as f:
            reader = csv.reader(f)
            for row in reader:
                if row[0] == "SKU":
                    return generate_schema_from_row(row, table_name)
                    break

    def generate_schema_from_row(row, table_name):
        print "Generating SQL Schema from CSV..."
        schema_sql = "create table " + table_name + "(\n"
        for column_title in row:
            if column_title in column_titles:
                schema_sql += column_titles[column_title]['name'] + ' ' + column_titles[column_title]['type'] + ",\n"
            else:
                schema_sql += ''.join(e for e in column_title if e.isalnum()) + " VARCHAR(200),\n"
        schema_sql = schema_sql[:-2]
        schema_sql += ");\n"
        return schema_sql

    def download_offer_file(offer_code_url):

        offer_code = offer_code_url.split('/')[4]

        base_url = "https://pricing.us-east-1.amazonaws.com"

        offer_code_url = offer_code_url[:-5] + ".csv"

        url = base_url + offer_code_url

        local_filename = "/tmp/" + offer_code + ".csv"

        file_exists = os.path.isfile(local_filename)

        if file_exists:
            resp = requests.head(url)
            md5_remote = resp.headers['etag'][1:-1]
            if md5(local_filename) == md5_remote:
                print("You already have the latest csv for " + offer_code + "!")
            else:
                download_file(url, local_filename)
        else:
            download_file(url, local_filename)

        file_exists = os.path.isfile(local_filename)

    def import_csv_into_mariadb(filename):

        table_name = filename[:-4]

        filename = "/tmp/" + filename

        schema = parse_csv_schema(filename, table_name)

        db = pymysql.connect(host=mariadb_host,
                              user=mariadb_user,
                              passwd=mariadb_password,
                              db=mariadb_db,
                              local_infile=1)

        cursor = db.cursor()
        load_data = "LOAD DATA LOCAL INFILE '" + filename + "' INTO TABLE " + table_name
        load_data += """ FIELDS TERMINATED BY ','
            ENCLOSED BY '"'
        LINES TERMINATED BY '\n'
        IGNORE 6 LINES; """

        print "Checking to see if table " + table_name + " exists..."
        cursor.execute("SELECT * FROM information_schema.tables WHERE table_schema = '" + mariadb_db + "' AND table_name = '" + table_name + "' LIMIT 1;")
        if cursor.fetchone() is not None:
            print "Dropping existing table " + table_name
            cursor.execute("DROP TABLE " + table_name + ";")
        print "Recreating table..."
        cursor.execute(schema)
        print "Loading csv data..."
        cursor.execute(load_data)
        db.commit()
        cursor.close()

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
    number_of_threads = 0
    for offer, offer_info in offer_index['offers'].iteritems():
        number_of_threads += 1
        filenames.append(offer + ".csv")
        urls.append(offer_info['currentVersionUrl'])

    for url in urls:
        download_offer_file(url)

    for filename in filenames:
        import_csv_into_mariadb(filename)
