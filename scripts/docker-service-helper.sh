#!/usr/bin/env bash
# Common helper functions for docker service management scripts

# Function to display usage
docker_service_usage() {
    local service_name="$1"
    echo "Usage: $0 [up|down|restart]"
    echo "  up      - Start ${service_name} (default)"
    echo "  down    - Stop ${service_name}"
    echo "  restart - Restart ${service_name}"
    exit 1
}

# Function to initialize environment and setup data directories
# Usage: docker_service_init [data_subdirs...]
docker_service_init() {
    # Use DATADIR environment variable if set, otherwise default to ./data
    local datadir="${DATADIR:-./data}"

    # Create base data directory
    mkdir -p "${datadir}"

    DATA_PATH=$(realpath "$datadir")
    export DATA_PATH

    # Create data subdirectories if any specified
    if [[ $# -gt 0 ]]; then
        for subdir in "$@"; do
            if [[ -n "$subdir" ]]; then
                mkdir -p "${datadir}/${subdir}"
            fi
        done
    fi

    return 0
}

# Function to execute docker compose action with custom messages
# Usage: docker_service_run <action> <compose_dir> <service_name> [compose_file] [info_lines...]
#   If compose_file is provided, it will be used with -f flag
docker_service_run() {
    local action="${1:-up}"
    local compose_dir="$2"
    local service_name="$3"
    local compose_file=""
    shift 3

    # Check if next argument looks like a compose file
    if [[ $# -gt 0 && "$1" =~ \.ya?ml$ ]]; then
        compose_file="$1"
        shift
    fi

    local info_lines=("$@")
    local compose_cmd="docker compose"

    if [[ -n "$compose_file" ]]; then
        compose_cmd="docker compose -f $compose_file"
    fi

    cd "$compose_dir" || exit 1

    # Validate and execute the requested action
    case "$action" in
        up)
            $compose_cmd up -d
            echo "${service_name} started"
            for line in "${info_lines[@]}"; do
                echo "$line"
            done
            return 0
            ;;
        down)
            $compose_cmd down
            echo "${service_name} stopped"
            return 0
            ;;
        restart)
            $compose_cmd restart
            echo "${service_name} restarted"
            for line in "${info_lines[@]}"; do
                echo "$line"
            done
            return 0
            ;;
        *)
            echo "Error: Invalid action '$action'" >&2
            echo "Usage: up|down|restart" >&2
            exit 1
            ;;
    esac
}
