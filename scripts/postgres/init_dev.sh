# use a local postgres install to create 2 dbs - teranode and coinbase
# don't be tempted to combine these into one db -ß both need a separate blocks table
psql -c "CREATE ROLE teranode LOGIN PASSWORD 'teranode' NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION; grant teranode to postgres;"
psql -c "CREATE DATABASE teranode WITH OWNER = teranode ENCODING = 'UTF8' CONNECTION LIMIT = -1;"
psql -c "CREATE DATABASE coinbase WITH OWNER = teranode ENCODING = 'UTF8' CONNECTION LIMIT = -1;"