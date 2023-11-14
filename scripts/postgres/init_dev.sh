# use a local postgres install to create 2 dbs - ubsv and coinbase
# don't be tempted to combine these into one db -ß both need a separate blocks table
psql -c "CREATE ROLE ubsv LOGIN PASSWORD 'ubsv' NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION; grant ubsv to postgres;"
psql -c "CREATE DATABASE ubsv WITH OWNER = ubsv ENCODING = 'UTF8' CONNECTION LIMIT = -1;"
psql -c "CREATE DATABASE coinbase WITH OWNER = ubsv ENCODING = 'UTF8' CONNECTION LIMIT = -1;"