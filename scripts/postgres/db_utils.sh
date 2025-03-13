#!/bin/bash

# Function to get PostgreSQL credentials
get_credentials() {
    # Check for environment variables first
    PGUSER=${PGUSER:-}
    PGPASSWORD=${PGPASSWORD:-}
    
    # If either is not set, prompt for them
    if [ -z "$PGUSER" ]; then
        read -p "Enter PostgreSQL username: " PGUSER
    fi
    
    if [ -z "$PGPASSWORD" ]; then
        read -s -p "Enter PostgreSQL password: " PGPASSWORD
        echo
    fi
    
    # Export for use in other scripts
    export PGUSER
    export PGPASSWORD
}

# Function to build PostgreSQL URL
get_pg_url() {
    local db_name=$1
    echo "postgres://$PGUSER:$PGPASSWORD@localhost:5432/$db_name"
}