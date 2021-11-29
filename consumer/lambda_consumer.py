import os
import sys
import re
import requests
import hashlib
import json
import boto3
import pymysql.cursors


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

    if "SQS_QUEUE" in os.environ:
        sqs_queue = os.environ["SQS_QUEUE"]
    else:
        sqs_queue = "aws_service_ingestor"


    column_titles = {
        "SKU": {
            "name": "SKU",
            "type": "CHAR(16) NOT NULL"
        },
        "OfferTermCode": {
            "name": "OfferTermCode",
            "type": "CHAR(10) NOT NULL"
        },
        "RateCode": {
            "name": "RateCode",
            "type": "CHAR(38) NOT NULL"
        },
        "TermType": {
            "name": "TermType",
            "type": "ENUM('OnDemand','Reserved') NOT NULL"
        },
        "PriceDescription": {
            "name": "PriceDescription",
            "type": "VARCHAR(140) NOT NULL"
        },
        "EffectiveDate": {
            "name": "EffectiveDate",
            "type": "DATE"
        },
        "StartingRange": {
            "name": "StartingRange",
            "type": "VARCHAR(15)"
        },
        "EndingRange": {
            "name": "EndingRange",
            "type": "VARCHAR(15)"
        },
        "Unit": {
            "name": "Unit",
            "type": "VARCHAR(40)"
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
            "type": "VARCHAR(5)"
        },
        "PurchaseOption": {
            "name": "PurchaseOption",
            "type": "VARCHAR(20)"
        },
        "OfferingClass": {
            "name": "OfferingClass",
            "type": "VARCHAR(11)"
        },
        "Product Family": {
            "name": "ProductFamily",
            "type": "VARCHAR(60)"
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
            "type": "VARCHAR(20)"
        },
        "Instance Type": {
            "name": "InstanceType",
            "type": "VARCHAR(50)"
        },
        "Current Generation": {
            "name": "CurrentGeneration",
            "type": "VARCHAR(3)"
        },
        "Instance Family": {
            "name": "InstanceFamily",
            "type": "VARCHAR(32)"
        },
        "Instance Type Family": {
            "name": "InstanceTypeFamily",
            "type": "VARCHAR(50)"
        },
        "Normalization Size Factor": {
            "name": "NormSizeFactor",
            "type": "VARCHAR(50)"
        },
        "vCPU": {
            "name": "vCPU",
            "type": "VARCHAR(4)"
        },
        "Physical Processor": {
            "name": "PhysicalProcessor",
            "type": "VARCHAR(50)"
        },
        "Clock Speed": {
            "name": "ClockSpeed",
            "type": "VARCHAR(15)"
        },
        "Memory": {
            "name": "Memory",
            "type": "VARCHAR(12)"
        },
        "Storage": {
            "name": "Storage",
            "type": "VARCHAR(30)"
        },
        "Network Performance": {
            "name": "NetworkPerformance",
            "type": "VARCHAR(20)"
        },
        "Processor Architecture": {
            "name": "ProcessorArchitecture",
            "type": "VARCHAR(20)"
        },
        "Storage Media": {
            "name": "StorageMedia",
            "type": "VARCHAR(20)"
        },
        "Volume Type": {
            "name": "VolumeType",
            "type": "VARCHAR(80)"
        },
        "Max Volume Size": {
            "name": "MaxVolumeSize",
            "type": "VARCHAR(10)"
        },
        "Max IOPS/volume": {
            "name": "MaxIOPSVolume",
            "type": "VARCHAR(30)"
        },
        "Max IOPS Burst Performance": {
            "name": "MaxIOPSBurstPerformance",
            "type": "VARCHAR(30)"
        },
        "Max throughput/volume": {
            "name": "MaxThroughputPerVolume",
            "type": "VARCHAR(15)"
        },
        "Provisioned": {
            "name": "Provisioned",
            "type": "ENUM('','No','Yes')"
        },
        "Tenancy": {
            "name": "Tenancy",
            "type": "VARCHAR(10)"
        },
        "EBS Optimized": {
            "name": "EBSOptimized",
            "type": "ENUM('','Yes')"
        },
        "Operating System": {
            "name": "OS",
            "type": "VARCHAR(40)"
        },
        "License Model": {
            "name": "LicenseModel",
            "type": "VARCHAR(50)"
        },
        "Group": {
            "name": "AWSGroup",
            "type": "VARCHAR(100)"
        },
        "Group Description": {
            "name": "AWSGroupDescription",
            "type": "VARCHAR(300)"
        },
        "Transfer Type": {
            "name": "TransferType",
            "type": "VARCHAR(200)"
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
            "type": "VARCHAR(60)"
        },
        "operation": {
            "name": "Operation",
            "type": "VARCHAR(40)"
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
            "type": "ENUM('','No','Yes')"
        },
        "GPU": {
            "name": "GPU",
            "type": "VARCHAR(3)"
        },
        "Instance Capacity - 10xlarge": {
            "name": "InstanceCapacity10xLarge",
            "type": "VARCHAR(2)"
        },
        "Instance Capacity - 2xlarge": {
            "name": "InstanceCapacity2xLarge",
            "type": "VARCHAR(2)"
        },
        "Instance Capacity - 4xlarge": {
            "name": "InstanceCapacity4xLarge",
            "type": "VARCHAR(2)"
        },
        "Instance Capacity - 8xlarge": {
            "name": "InstanceCapacity8xLarge",
            "type": "VARCHAR(2)"
        },
        "Instance Capacity - large": {
            "name": "InstanceCapacityLarge",
            "type": "VARCHAR(2)"
        },
        "Instance Capacity - medium": {
            "name": "InstanceCapacityMedium",
            "type": "VARCHAR(2)"
        },
        "Instance Capacity - xlarge": {
            "name": "InstanceCapacityxLarge",
            "type": "VARCHAR(2)"
        },
        "Intel AVX Available": {
            "name": "IntelAVXAvailable",
            "type": "VARCHAR(4)"
        },
        "Intel AVX2 Available": {
            "name": "IntelAVX2Available",
            "type": "VARCHAR(4)"
        },
        "Intel Turbo Available": {
            "name": "IntelTurboAvailable",
            "type": "VARCHAR(4)"
        },
        "Physical Cores": {
            "name": "PhysicalCores",
            "type": "VARCHAR(3)"
        },
        "Pre Installed S/W": {
            "name": "PreInstalledSW",
            "type": "VARCHAR(10)"
        },
        "Processor Features": {
            "name": "ProcessorFeatures",
            "type": "VARCHAR(200)"
        },
        "Sockets": {
            "name": "Sockets",
            "type": "VARCHAR(10)"
        }
    }

    def parse_csv_schema(file_handle, table_name):
        file_handle.seek(0, 0)
        for l in file_handle:
            l = l.decode("utf-8")
            if l[:5] == '"SKU"':
                schema = generate_schema_from_row(l.split(','), table_name)
                return schema


    def generate_schema_from_row(row, table_name):
        print("Generating SQL Schema from CSV...")
        schema_sql = "CREATE TABLE " + table_name + "(\n"
        for column_title in row:
            column_title = column_title.strip('\"')
            if column_title in column_titles:
                schema_sql += '`' + column_titles[column_title]['name'] + '` ' + column_titles[column_title]['type'] + ",\n"
            else:
                schema_friendly_column_title = ""
                for character in column_title:
                    if re.match(r'[0-9A-Za-z\s]', character):
                        if character == " ":
                            character = "_"
                        schema_friendly_column_title += character

                schema_sql += '`' + schema_friendly_column_title.rstrip("\n") + '`' + " VARCHAR(200),\n"
        # add below for md5 in database
        # schema_sql += "MD5 VARCHAR(33),\n"
        schema_sql = schema_sql[:-2]
        schema_sql += ") CHARACTER SET 'utf8';\n"
        return schema_sql


    def process_offer(url, csv_file):

        db = pymysql.connect(host=mariadb_host,
                             user=mariadb_user,
                             passwd=mariadb_password,
                             db=mariadb_db,
                             charset='utf8',
                             ssl_verify_cert=1)

        cursor = db.cursor()

        table_name = url.split('/')[6]

        print("Processing Offer " + table_name)

        print("Downloading chunks of " + url + "...")
        response = requests.get(url, stream=True)

        resp = requests.head(url)
        file_size = resp.headers['Content-Length']

        csv_header = b''

        file_number = 0
        total_written = 0
        file_written = 0
        truncated_text = ""
        new_file = False

        # 4MB Chunks
        for chunk in response.iter_content(chunk_size=4194304):
            if new_file is True:
                # Empty file
                csv_file.seek(0)
                csv_file.truncate()

                csv_file.write(csv_header)
                csv_file.write(truncated_text)
                new_file = False

            # Progress indicator
            percent_done = round((int(total_written) / int(file_size)) * 100, 2)
            print("Downloaded:", percent_done, "%")
            written = csv_file.write(chunk)
            total_written = total_written + written

            csv_header_found = False
            truncated_string_found = False
            drop_database = False

            file_written = file_written + written
            position = 0

            # Limit local filesize to 384MB
            if file_written > 402653184:
                file_written = 0
                file_number += 1
                new_file = True

                if int(total_written) == int(file_size):
                    print("Downloaded: 100 %")

                # get CSV header
                if file_number == 1:
                    drop_database = True
                    # goto the beginning of the file
                    csv_file.seek(0, 0)
                    while csv_header_found is False:
                        l = csv_file.readline()
                        decoded_l = l.decode("utf-8")
                        if decoded_l[:5] == '"SKU"':
                            csv_header = l
                            csv_header_found = True

                # Find first newline from end of file
                while truncated_string_found is False:
                    # goto the last character of the file
                    csv_file.seek(-position, 2)
                    # Read one character at a time
                    char = csv_file.read(1)
                    if char.decode("utf-8") == '\n':
                        truncated_text = csv_file.read()

                        # Move 1 character forward, we want to retain the newline
                        csv_file.seek(-position + 1, 2)
                        csv_file.truncate()
                        import_csv_into_mariadb(filepath, table_name, drop_database, csv_file)

                        # Empty file
                        csv_file.seek(0)
                        csv_file.truncate()
                        truncated_string_found = True

                    position += 1

            if int(total_written) == int(file_size):
                if file_number >= 1:
                    drop_db = False
                else:
                    drop_db = True
                print("Downloaded: 100 %")
                import_csv_into_mariadb(filepath, table_name, drop_db, csv_file)
                if table_name == "AmazonEC2":
                    print("Creating index on AmazonEC2 table")
                    cursor.execute("CREATE INDEX ec2_index ON AmazonEC2 (TermType, Location, InstanceType, Tenancy, OS, CapacityStatus, PreInstalledSW);")
                csv_file.seek(0)
                csv_file.truncate()


    def import_csv_into_mariadb(filename, table_name, drop_database, csv_file):

        db = pymysql.connect(host=mariadb_host,
                             user=mariadb_user,
                             passwd=mariadb_password,
                             db=mariadb_db,
                             charset='utf8',
                             ssl_verify_cert=1)

        cursor = db.cursor()

        print("Checking to see if table " + table_name + " exists...")
        cursor.execute("SELECT * FROM information_schema.tables WHERE table_schema = '" + mariadb_db + "' AND table_name = '" + table_name + "' LIMIT 1;")
        if cursor.fetchone() is not None:
            if drop_database is True:
                schema = parse_csv_schema(csv_file, table_name)
                print("Dropping existing table " + table_name)
                cursor.execute("DROP TABLE " + table_name + ";")
                print("Recreating table...")
                try:
                    cursor.execute(schema)
                except pymysql.Error as e:
                    print(schema)
                    print("ERROR: Error recreating table: ", e)
                    sys.exit(1)
        else:
            schema = parse_csv_schema(csv_file, table_name)
            print("Creating table...")
            try:
                cursor.execute(schema)
            except pymysql.Error as e:
                print(schema)
                print("ERROR: Error creating table: ", e)
                sys.exit(1)
        print("Loading csv data...")
        print()

        load_data = ""
        number_of_rows_to_skip = 1
        csv_file.seek(0, 0)
        if drop_database is True:
            number_of_rows_to_skip = 6

        row_counter = 0
        rows = ""
        for row in csv_file:
            row = row.decode("utf-8")
            row = row.rstrip("\n")
            while ",," in row:
                row = row.replace(",,", ",NULL,")
            if row.endswith(","):
                row = row + "NULL"
            row_counter = row_counter + 1
            if row_counter <= number_of_rows_to_skip:
                continue
            elif row_counter == number_of_rows_to_skip + 1:
                rows = "(" + row + ")"
            else:
                batch_size = 1000
                # Insert data in batches of batch_size rows
                if row_counter % batch_size == 0:
                    rows = rows + ", (" + row + ")"
                    load_data = "INSERT INTO " + table_name + " VALUES " + rows + ";"
                    try:
                        cursor.execute(load_data)
                    except pymysql.Error as e:
                        print(load_data)
                        print("ERROR: Error executing query: ", e)
                        sys.exit(1)

                    rows = ""
                elif row_counter % batch_size == 1:
                    rows = "(" + row + ")"
                else:
                    rows = rows + ", (" + row + ")"

        load_data = "INSERT INTO " + table_name + " VALUES " + rows + ";"
        try:
            cursor.execute(load_data)
        except pymysql.Error as e:
            print(load_data)
            print("ERROR: Error executing query: ", e)
            sys.exit(1)

        db.commit()
        cursor.close()


    filepath = "/tmp/working_copy.csv"

    csv_file_handle = open(filepath, "w+b")

    all_records = event['Records']

    for record in all_records:
        csv_url = record['body']
        receipt_handle = record['receiptHandle']

        process_offer(csv_url, csv_file_handle)

        # Create SQS client
        sqs = boto3.client('sqs')

        print("Deleting SQS Message...")

        response = sqs.delete_message(
            QueueUrl=sqs_queue,
            ReceiptHandle=receipt_handle
        )

        print(response)
