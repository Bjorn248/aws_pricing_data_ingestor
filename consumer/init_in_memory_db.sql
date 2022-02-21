-- Creates a database and user in the in-memory cockroach instance for local development
CREATE DATABASE cashier;
CREATE USER cashier WITH PASSWORD cashier;
GRANT ALL ON DATABASE cashier TO cashier;
