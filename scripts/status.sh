#!/bin/bash

# Check status of Databricks jobs
# Usage: ./scripts/status.sh [JOB_NAME]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to get job ID from file
get_job_id() {
    local job_name="$1"
    local job_id_file=".databricks/job-ids/$job_name.txt"
    
    if [ -f "$job_id_file" ]; then
        cat "$job_id_file"
    else
        echo -e "${RED}❌ Job ID file not found: $job_id_file${NC}"
        echo "Please run './scripts/deploy.sh --job $job_name' first"
        exit 1
    fi
}

# Function to get latest run ID
get_latest_run_id() {
    local job_name="$1"
    local run_id_file=".databricks/job-ids/$job_name-latest-run.txt"
    
    if [ -f "$run_id_file" ]; then
        cat "$run_id_file"
    else
        echo ""
    fi
}

# Function to check job status
check_job_status() {
    local job_name="$1"
    local job_id=$(get_job_id "$job_name")
    
    echo -e "${BLUE}Job: $job_name (ID: $job_id)${NC}"
    
    # Get job details
    local job_info=$(databricks jobs get --job-id "$job_id")
    echo -e "${YELLOW}Job Details:${NC}"
    echo "$job_info" | jq '.settings | {name, max_concurrent_runs, timeout_seconds, tags}'
    
    # Get latest runs
    echo -e "\n${YELLOW}Recent Runs:${NC}"
    local runs=$(databricks runs list --job-id "$job_id" --limit 5)
    echo "$runs" | jq '.runs[] | {run_id, state: .state.life_cycle_state, start_time, end_time}'
    
    # Get latest run details if available
    local latest_run_id=$(get_latest_run_id "$job_name")
    if [ -n "$latest_run_id" ]; then
        echo -e "\n${YELLOW}Latest Run Details (ID: $latest_run_id):${NC}"
        local run_details=$(databricks runs get --run-id "$latest_run_id")
        echo "$run_details" | jq '{run_id, state: .state, start_time, end_time, run_duration}'
    fi
}

# Function to list all jobs
list_all_jobs() {
    echo -e "${BLUE}All Deployed Jobs:${NC}"
    
    for job_file in .databricks/job-ids/*.txt; do
        if [ -f "$job_file" ]; then
            local job_name=$(basename "$job_file" .txt)
            local job_id=$(cat "$job_file")
            
            echo -e "\n${YELLOW}--- $job_name ---${NC}"
            echo "Job ID: $job_id"
            
            # Get basic job info
            local job_info=$(databricks jobs get --job-id "$job_id" 2>/dev/null || echo "{}")
            if [ "$job_info" != "{}" ]; then
                local job_state=$(echo "$job_info" | jq -r '.settings.name // "Unknown"')
                echo "Status: Active"
            else
                echo "Status: Not found or deleted"
            fi
        fi
    done
}

# Main execution
if [ $# -eq 0 ]; then
    list_all_jobs
else
    JOB_NAME="$1"
    case "$JOB_NAME" in
        "setup-environment"|"simulate-data"|"data-pipeline")
            check_job_status "$JOB_NAME"
            ;;
        *)
            echo -e "${RED}❌ Unknown job name: $JOB_NAME${NC}"
            echo "Available jobs: setup-environment, simulate-data, data-pipeline"
            exit 1
            ;;
    esac
fi
