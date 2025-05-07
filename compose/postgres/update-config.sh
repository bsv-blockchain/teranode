#!/bin/bash
set -e

# Used by docker-compose to update postgresql.conf

# Directly modify the postgresql.conf to adjust max_connections
echo "max_connections = '1000'" >> /var/lib/postgresql/data/postgresql.conf
echo "lock_timeout = '0'" >> /var/lib/postgresql/data/postgresql.conf
echo "port = '15432'" >> /var/lib/postgresql/data/postgresql.conf

# Performance boost at the cost of consistency if shutdown unexpectedly
echo "synchronous_commit = 'off'" >> /var/lib/postgresql/data/postgresql.conf
echo "fsync = 'off'" >> /var/lib/postgresql/data/postgresql.conf
echo "full_page_writes = 'off'" >> /var/lib/postgresql/data/postgresql.conf

# Alternatively, uncomment include_dir if you have custom configurations in a directory
# sed -i "/include_dir = 'conf.d'/s/^#//g" /var/lib/postgresql/data/postgresql.conf
