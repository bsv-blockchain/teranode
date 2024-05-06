#!/bin/bash
set -e

# Directly modify the postgresql.conf to adjust max_connections
echo "max_connections = '1000'" >> /var/lib/postgresql/data/postgresql.conf

# Alternatively, uncomment include_dir if you have custom configurations in a directory
# sed -i "/include_dir = 'conf.d'/s/^#//g" /var/lib/postgresql/data/postgresql.conf
