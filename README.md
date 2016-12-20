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
