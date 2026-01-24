#!/bin/bash
#
# Atomic Rename Demo for FractalBits
#
# This script demonstrates the performance advantage of FractalBits' atomic rename
# operations compared to traditional copy+delete approaches.
#
# Usage:
#   ./atomic_rename_demo.sh [object|folder|both]
#
# Prerequisites:
#   - FractalBits services running (cargo xtask service start)
#   - fractal-s3 CLI built (cargo build -p fractal-s3)
#   - AWS CLI installed (for comparison)

set -e

# Configuration
export AWS_DEFAULT_REGION=localdev
export AWS_ENDPOINT_URL_S3=http://localhost:8080
export AWS_ACCESS_KEY_ID=test_api_key
export AWS_SECRET_ACCESS_KEY=test_api_secret

FRACTAL_S3="./target/debug/fractal-s3"
DEMO_BUCKET="atomic-rename-demo"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_header() {
    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN} $1${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo ""
}

cleanup() {
    log_info "Cleaning up demo bucket..."
    $FRACTAL_S3 rm "s3://${DEMO_BUCKET}" -r -q 2>/dev/null || true
    $FRACTAL_S3 rb "s3://${DEMO_BUCKET}" 2>/dev/null || true
    rm -f /tmp/demo_large_file_* 2>/dev/null || true
}

setup_bucket() {
    log_info "Creating demo bucket: ${DEMO_BUCKET}"
    $FRACTAL_S3 mb "s3://${DEMO_BUCKET}" 2>/dev/null || true
}

# Demo 1: Large Object Rename
demo_object_rename() {
    local size_mb=${1:-100}  # Default 100MB, can pass 5120 for 5GB
    local size_bytes=$((size_mb * 1024 * 1024))

    log_header "Demo: Atomic Object Rename (${size_mb}MB object)"

    # Create large file
    log_info "Creating ${size_mb}MB test file..."
    dd if=/dev/urandom of=/tmp/demo_large_file_${size_mb}mb bs=1M count=${size_mb} 2>/dev/null

    # Upload
    log_info "Uploading ${size_mb}MB file to S3..."
    upload_start=$(date +%s%3N)
    $FRACTAL_S3 cp "/tmp/demo_large_file_${size_mb}mb" "s3://${DEMO_BUCKET}/large_object_original.bin"
    upload_end=$(date +%s%3N)
    upload_time=$(( (upload_end - upload_start) ))
    upload_time_sec=$(awk "BEGIN {printf \"%.3f\", $upload_time/1000}")
    log_info "Upload completed in ${upload_time_sec}s"

    # Atomic rename with fractal-s3
    log_info "Performing atomic rename with fractal-s3..."
    rename_start=$(date +%s%3N)
    $FRACTAL_S3 mv "s3://${DEMO_BUCKET}/large_object_original.bin" "s3://${DEMO_BUCKET}/large_object_renamed.bin"
    rename_end=$(date +%s%3N)
    rename_time=$(( (rename_end - rename_start) ))
    rename_time_sec=$(awk "BEGIN {printf \"%.3f\", $rename_time/1000}")

    log_success "Atomic rename completed in ${rename_time_sec}s"

    echo ""
    echo -e "  ${YELLOW}Results:${NC}"
    echo -e "  - Object size: ${size_mb}MB"
    echo -e "  - Upload time: ${upload_time_sec}s"
    echo -e "  - Rename time: ${GREEN}${rename_time_sec}s${NC} (instant, regardless of size!)"

    # Verify rename: show listing of old and new locations
    echo ""
    echo -e "  ${YELLOW}Listing old location (s3://${DEMO_BUCKET}/large_object_original.bin):${NC}"
    old_listing=$($FRACTAL_S3 ls "s3://${DEMO_BUCKET}/large_object_original.bin" 2>/dev/null || true)
    if [ -z "$old_listing" ]; then
        echo -e "    ${GREEN}(no objects found)${NC}"
    else
        echo "$old_listing" | sed 's/^/    /'
    fi

    echo ""
    echo -e "  ${YELLOW}Listing new location (s3://${DEMO_BUCKET}/large_object_renamed.bin):${NC}"
    $FRACTAL_S3 ls "s3://${DEMO_BUCKET}/large_object_renamed.bin" 2>/dev/null | sed 's/^/    /'
    echo ""

    # Cleanup this object
    $FRACTAL_S3 rm "s3://${DEMO_BUCKET}/large_object_renamed.bin" >/dev/null 2>&1 || true
    rm -f "/tmp/demo_large_file_${size_mb}mb"
}

# Demo 2: Folder with Many Objects Rename
demo_folder_rename() {
    local num_files=${1:-1000}  # Default 1000 files, can pass 10000 for 10k

    log_header "Demo: Atomic Folder Rename (${num_files} objects)"

    # Create folder structure with many files
    log_info "Creating ${num_files} small files locally..."
    mkdir -p /tmp/demo_folder_${num_files}
    for i in $(seq 1 $num_files); do
        echo "content-$i" > "/tmp/demo_folder_${num_files}/file_$i.txt"
    done

    # Upload all files
    log_info "Uploading ${num_files} files to S3 (this may take a while)..."
    upload_start=$(date +%s%3N)
    $FRACTAL_S3 cp -r "/tmp/demo_folder_${num_files}" "s3://${DEMO_BUCKET}/original_folder/"
    upload_end=$(date +%s%3N)
    upload_time=$(( (upload_end - upload_start) ))
    upload_time_sec=$(awk "BEGIN {printf \"%.3f\", $upload_time/1000}")
    log_info "Upload completed in ${upload_time_sec}s"

    # Count objects
    object_count=$($FRACTAL_S3 ls "s3://${DEMO_BUCKET}/original_folder/" --recursive 2>/dev/null | wc -l)
    log_info "Uploaded ${object_count} objects"

    # Atomic folder rename with fractal-s3
    log_info "Performing atomic folder rename with fractal-s3..."
    rename_start=$(date +%s%3N)
    $FRACTAL_S3 mv "s3://${DEMO_BUCKET}/original_folder/" "s3://${DEMO_BUCKET}/renamed_folder/"
    rename_end=$(date +%s%3N)
    rename_time=$(( (rename_end - rename_start) ))
    rename_time_sec=$(awk "BEGIN {printf \"%.3f\", $rename_time/1000}")

    log_success "Atomic folder rename completed in ${rename_time_sec}s"

    echo ""
    echo -e "  ${YELLOW}Results:${NC}"
    echo -e "  - Number of objects: ${num_files}"
    echo -e "  - Upload time: ${upload_time_sec}s"
    echo -e "  - Rename time: ${GREEN}${rename_time_sec}s${NC} (instant, regardless of object count!)"

    # Verify rename: show listing of old and new locations
    echo ""
    echo -e "  ${YELLOW}Listing old location (s3://${DEMO_BUCKET}/original_folder/):${NC}"
    old_listing=$($FRACTAL_S3 ls "s3://${DEMO_BUCKET}/original_folder/" --recursive 2>/dev/null || true)
    if [ -z "$old_listing" ]; then
        echo -e "    ${GREEN}(no objects found)${NC}"
    else
        echo "$old_listing" | sed 's/^/    /'
    fi

    echo ""
    echo -e "  ${YELLOW}Listing new location (s3://${DEMO_BUCKET}/renamed_folder/):${NC}"
    new_listing=$($FRACTAL_S3 ls "s3://${DEMO_BUCKET}/renamed_folder/" --recursive 2>/dev/null)
    new_count=$(echo "$new_listing" | wc -l)
    echo "$new_listing" | head -5 | sed 's/^/    /'
    if [ "$new_count" -gt 5 ]; then
        echo -e "    ... (${new_count} objects total)"
    fi
    echo ""

    # Compare with what copy+delete would take (estimate)
    estimated_copy_time=$(awk "BEGIN {printf \"%.1f\", $upload_time_sec * 2}")
    log_info "Traditional copy+delete would take approximately ${estimated_copy_time}s (2x upload time)"

    # Cleanup
    rm -rf "/tmp/demo_folder_${num_files}"
}

# Main
main() {
    local demo_type=${1:-both}

    echo ""
    echo -e "${GREEN}================================================${NC}"
    echo -e "${GREEN}  FractalBits Atomic Rename Performance Demo${NC}"
    echo -e "${GREEN}================================================${NC}"
    echo ""

    # Cleanup any previous demo data
    cleanup

    # Setup
    setup_bucket

    case $demo_type in
        object)
            demo_object_rename ${2:-100}
            ;;
        folder)
            demo_folder_rename ${2:-1000}
            ;;
        both)
            demo_object_rename ${2:-100}
            demo_folder_rename ${3:-1000}
            ;;
        large)
            # Large demo: 1GB object, 10k files
            demo_object_rename 1024
            demo_folder_rename 10000
            ;;
        xlarge)
            # Extra large demo: 5GB object, 100k files (warning: takes a long time to upload)
            demo_object_rename 5120
            demo_folder_rename 100000
            ;;
        *)
            echo "Usage: $0 [object|folder|both|large|xlarge] [size_mb|num_files]"
            echo ""
            echo "Examples:"
            echo "  $0 object 500      # Demo with 500MB object"
            echo "  $0 folder 5000     # Demo with 5000 files"
            echo "  $0 both            # Both demos with defaults (100MB, 1000 files)"
            echo "  $0 large           # Large demo (1GB object, 10k files)"
            echo "  $0 xlarge          # Extra large (5GB object, 100k files)"
            exit 1
            ;;
    esac

    # Final cleanup
    cleanup

    log_header "Demo Complete!"
    echo "Key takeaways:"
    echo "  1. Atomic object rename is O(1) - instant regardless of object size"
    echo "  2. Atomic folder rename is O(1) - instant regardless of number of objects"
    echo "  3. Traditional copy+delete is O(n) - proportional to data size"
    echo ""
}

main "$@"
