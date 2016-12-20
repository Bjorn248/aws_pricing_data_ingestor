# aws_pricing_data_ingestor
Python script to download data from the AWS Pricing API and import it into MariaDB

## Environment Variables
Variable Name | Description
------------ | -------------
MARIADB_HOST | The hostname/ip of your MariaDB Instance (e.g. localhost)
MARIADB_USER | The user with which to authenticate to MariaDB
MARIADB_PASSWORD | The password with which to authenticate to MariaDB
MARIADB_DB | The DB name to connect to (e.g. aws_prices)

## Instructions
To run this script, make sure you have all environment variables set and have
a target MariaDB database running. Then, simply run the script using `./pricing_import.py`.

## How it works
This pulls all the CSV files directly from the [AWS Pricing List API](https://aws.amazon.com/blogs/aws/new-aws-price-list-api/)
and imports them directly into MariaDB using `LOAD DATA LOCAL INFILE`. The data
is not modified in any way. Schemas are generated from the CSVs. Every time the
script runs, the table is dropped and recreated (in case a new column was added
by AWS).
