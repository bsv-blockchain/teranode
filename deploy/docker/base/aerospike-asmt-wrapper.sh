#!/bin/bash
# Aerospike ASMT (Shared Memory Tool) Wrapper Script
# This script automates index backup/restore for persistent secondary indexes
# across Docker container restarts using shared memory (shmem) indexes.

set -euo pipefail

# Configuration
BACKUP_DIR="/backup/indexes"
NAMESPACE="utxo-store"
AEROSPIKE_CONFIG="/etc/aerospike.conf"
USE_ASMT=${USE_ASMT:-true}  # Can disable via environment variable

# Logging functions
log_info() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 📋 INFO: $*"
}

log_success() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✅ SUCCESS: $*"
}

log_warn() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ⚠️ WARNING: $*"
}

log_error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ❌ ERROR: $*"
}

# Install ASMT tool if not present
install_asmt() {
    local asmt_version="2.1.4"
    local arch_suffix

    # Detect architecture for DEB package
    case "$(uname -m)" in
        x86_64) arch_suffix="amd64" ;;
        aarch64) arch_suffix="arm64" ;;
        *)
            log_error "Unsupported architecture: $(uname -m)"
            return 1
            ;;
    esac

    # Try Ubuntu 24.04 first, fallback to other versions if needed
    local asmt_urls=(
        "https://dl.aerospike.com/artifacts/asmt/${asmt_version}/asmt_${asmt_version}-1ubuntu24.04_${arch_suffix}.deb"
        "https://dl.aerospike.com/artifacts/asmt/${asmt_version}/asmt_${asmt_version}-1ubuntu22.04_${arch_suffix}.deb"
        "https://dl.aerospike.com/artifacts/asmt/${asmt_version}/asmt_${asmt_version}-1ubuntu20.04_${arch_suffix}.deb"
    )
    local temp_dir="/tmp/asmt-install"

    log_info "Installing ASMT version ${asmt_version} for ${arch_suffix}..."

    # Create temp directory
    mkdir -p "$temp_dir"
    cd "$temp_dir" || return 1

    # Try downloading from each URL until one works
    for asmt_url in "${asmt_urls[@]}"; do
        log_info "Trying to download from: $asmt_url"

        if curl -L -o asmt.deb "$asmt_url" 2>/dev/null; then
            log_info "Downloaded ASMT DEB package successfully"

            # Install the package
            if dpkg -i asmt.deb 2>/dev/null; then
                log_success "ASMT installed via dpkg"
                # Cleanup
                cd / && rm -rf "$temp_dir"
                return 0
            else
                log_warn "dpkg installation failed, trying manual extraction..."

                # Try manual extraction as fallback
                if ar x asmt.deb 2>/dev/null && tar -xf data.tar.* 2>/dev/null; then
                    # Find the binary in the extracted files
                    local asmt_binary
                    asmt_binary=$(find . -name "asmt" -type f 2>/dev/null | head -n1)
                    if [ -n "$asmt_binary" ]; then
                        cp "$asmt_binary" /usr/local/bin/asmt
                        chmod +x /usr/local/bin/asmt
                        log_success "ASMT extracted and installed manually"

                        # Cleanup
                        cd / && rm -rf "$temp_dir"
                        return 0
                    fi
                fi
                log_warn "Manual extraction failed for this package, trying next URL..."
            fi
        else
            log_warn "Failed to download from $asmt_url, trying next..."
        fi

        # Clean up failed download
        rm -f asmt.deb
    done

    log_error "Failed to download ASMT from any URL"

    # Cleanup on failure
    cd / && rm -rf "$temp_dir"
    return 1
}

# Check if ASMT is enabled and configuration uses shmem
should_use_asmt() {
    log_info "should_use_asmt() called"
    if [ "$USE_ASMT" != "true" ]; then
        log_info "ASMT disabled via USE_ASMT environment variable"
        return 1
    fi

    if [ ! -f "$AEROSPIKE_CONFIG" ]; then
        log_warn "Config file not found: $AEROSPIKE_CONFIG"
        return 1
    fi

    # Check if config uses shmem indexes (default in EE)
    # If no explicit index-type, EE defaults to shmem
    if grep -q "index-type.*flash\|sindex-type.*flash" "$AEROSPIKE_CONFIG"; then
        log_info "Flash indexes detected - ASMT not needed"
        return 1
    fi

    # Check if asmt tool is available, install if needed
    if ! command -v asmt >/dev/null 2>&1; then
        log_info "ASMT tool not found - attempting to install..."
        install_asmt
        if ! command -v asmt >/dev/null 2>&1; then
            log_warn "ASMT installation failed - using EE shared memory persistence only"
            return 1
        fi
        log_success "ASMT tool installed successfully"
    fi

    log_info "Shared memory indexes detected and ASMT available - ASMT enabled"
    return 0
}

# Restore indexes from backup on startup
pre_start_restore() {
    if ! should_use_asmt; then
        log_info "Skipping ASMT restore - cold starting"
        return 0
    fi

    if [ ! -d "$BACKUP_DIR" ] || [ -z "$(ls -A "$BACKUP_DIR" 2>/dev/null)" ]; then
        log_info "No backup found in $BACKUP_DIR - cold starting"
        return 0
    fi

    log_info "Found backup - attempting warm start restore..."

    # Restore shared memory segments with timeout
    if timeout 120 asmt -r -v -p "$BACKUP_DIR" -n "$NAMESPACE" 2>&1 | tee /tmp/asmt-restore.log; then
        log_success "Index restore successful - warm start ready!"
        log_info "Backup contents: $(ls -lah "$BACKUP_DIR" | tail -n +2 | wc -l) files, $(du -sh "$BACKUP_DIR" | cut -f1) total"
        # Mark successful restore for later cleanup
        touch /tmp/restore-successful
        return 0
    else
        log_error "Index restore failed - cleaning corrupted backup"
        cat /tmp/asmt-restore.log || true
        rm -rf "$BACKUP_DIR"/*
        log_info "Falling back to cold start"
        return 1
    fi
}

# Backup indexes to disk before shutdown
post_stop_backup() {
    log_info "post_stop_backup() called"
    if ! should_use_asmt; then
        log_info "Skipping ASMT backup"
        return 0
    fi

    log_info "Creating index backup before shutdown..."
    mkdir -p "$BACKUP_DIR"

    # Create backup metadata
    cat > "$BACKUP_DIR/metadata.json" << EOF
{
    "timestamp": "$(date -Iseconds)",
    "namespace": "$NAMESPACE",
    "hostname": "$(hostname)",
    "aerospike_version": "$(asd --version 2>/dev/null | head -n1 || echo 'unknown')"
}
EOF

    # Backup shared memory segments with timeout
    if timeout 120 asmt -b -v -p "$BACKUP_DIR" -n "$NAMESPACE" 2>&1 | tee /tmp/asmt-backup.log; then
        local backup_size
        backup_size=$(du -sh "$BACKUP_DIR" 2>/dev/null | cut -f1 || echo "unknown")
        log_success "Index backup complete - size: $backup_size"
        log_info "Backup location: $BACKUP_DIR"
        ls -lah "$BACKUP_DIR" || true
        return 0
    else
        log_error "Index backup failed - removing partial backup"
        cat /tmp/asmt-backup.log || true
        rm -rf "$BACKUP_DIR"/*
        return 1
    fi
}

# Graceful shutdown handler
# shellcheck disable=SC2120
shutdown_handler() {
    local signal=${1:-"UNKNOWN"}
    log_info "Received $signal signal - initiating graceful shutdown..."

    if [ -n "${AEROSPIKE_PID:-}" ] && kill -0 "$AEROSPIKE_PID" 2>/dev/null; then
        log_info "Stopping Aerospike process (PID: $AEROSPIKE_PID)..."

        # Send SIGTERM to Aerospike
        log_info "Sending SIGTERM to Aerospike..."
        kill -TERM "$AEROSPIKE_PID" 2>/dev/null || true

        # Wait for Aerospike to exit naturally (no timeout loop)
        log_info "Waiting for Aerospike to exit..."
        wait "$AEROSPIKE_PID" 2>/dev/null || true
        log_success "Aerospike process has exited"
    fi

    log_info "Process handling complete - continuing to backup phase..."

    # Run post-stop backup
    log_info "About to call post_stop_backup()..."
    post_stop_backup
    log_info "Returned from post_stop_backup()"

    log_info "Shutdown complete"
    exit 0
}

# Set up signal handlers for graceful shutdown
trap 'shutdown_handler SIGTERM' SIGTERM
trap 'shutdown_handler SIGINT' SIGINT
trap 'shutdown_handler SIGQUIT' SIGQUIT

# Main execution
main() {
    log_info "=== Aerospike ASMT Wrapper Starting ==="
    log_info "Namespace: $NAMESPACE"
    log_info "Backup directory: $BACKUP_DIR"
    log_info "Config file: $AEROSPIKE_CONFIG"

    # Pre-start: Attempt to restore indexes
    pre_start_restore

    # Start Aerospike in the background
    log_info "Starting Aerospike daemon..."
    /usr/bin/asd --config-file "$AEROSPIKE_CONFIG" --foreground &
    AEROSPIKE_PID=$!

    log_success "Aerospike started with PID: $AEROSPIKE_PID"

    # Wait for Aerospike to become ready (increased timeout for ASMT restore)
    local ready_timeout=300  # 5 minutes to handle large index restores
    local ready_count=0
    log_info "Waiting for Aerospike to become ready (max ${ready_timeout}s)..."

    # Give Aerospike a moment to fully initialize before we start poking it
    sleep 5

    while [ $ready_count -lt $ready_timeout ]; do
        # Check if Aerospike process is still running
        if ! kill -0 "$AEROSPIKE_PID" 2>/dev/null; then
            log_error "Aerospike process died during startup (PID $AEROSPIKE_PID no longer exists)"
            return 1
        fi

        # Test if asinfo command works without causing issues
        local asinfo_result
        asinfo_result=$(asinfo -v "status" -p 3000 2>&1)
        local asinfo_exit=$?

        if [ $asinfo_exit -eq 0 ] && echo "$asinfo_result" | grep -q "ok"; then
            log_success "Aerospike is ready and accepting connections after $((ready_count + 5))s"

            # Clean up stale backup after successful warm start
            if [ -f /tmp/restore-successful ]; then
                log_info "Cleaning up stale backup - indexes are now in memory and will diverge"
                rm -rf "$BACKUP_DIR"/*
                rm -f /tmp/restore-successful
                log_success "Stale backup cleaned up"
            fi
            break
        fi

        # Log progress every 10 seconds during startup
        if [ $((ready_count % 10)) -eq 0 ] && [ $ready_count -gt 0 ]; then
            log_info "Still waiting for Aerospike startup... ${ready_count}s elapsed"
        fi

        sleep 1
        ((ready_count++))
    done

    if [ $ready_count -ge $ready_timeout ]; then
        log_error "Aerospike readiness check timed out after ${ready_timeout}s - this may indicate ASMT restore issues"
        return 1
    fi

    # Wait for Aerospike process to exit
    wait "$AEROSPIKE_PID"
    local exit_code=$?

    log_info "Aerospike process exited with code: $exit_code"

    # If we get here without a signal, something went wrong
    if [ $exit_code -ne 0 ]; then
        log_error "Aerospike exited unexpectedly with code $exit_code"

        # Check for common causes
        if [ $exit_code -eq 1 ]; then
            log_error "Exit code 1 usually indicates:"
            log_error "  - Out of memory during startup"
            log_error "  - Corrupted data files or indexes"
            log_error "  - Configuration errors"
            log_error "  - File permission issues"

            # Check memory usage
            log_info "Current memory usage:"
            free -h || true

            # Check disk space
            log_info "Disk space:"
            df -h /opt/aerospike/ || true

            # Check for core dumps or error logs
            log_info "Checking for additional error information..."
            if [ -f /var/log/aerospike/aerospike.log ]; then
                log_info "Last 10 lines from aerospike.log:"
                tail -10 /var/log/aerospike/aerospike.log || true
            fi
        fi

        # Don't backup if Aerospike crashed
        log_warn "Skipping backup due to unexpected Aerospike exit"
        exit $exit_code
    fi

    # Run backup before final exit
    post_stop_backup

    exit $exit_code
}

# Execute main function
main "$@"
