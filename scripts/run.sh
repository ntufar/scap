#!/bin/bash

# Run Databricks jobs using Databricks CLI
# Usage: ./scripts/run.sh JOB_NAME [--params JSON_PARAMS]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Check if job name is provided
if [ $# -lt 1 ]; then
    echo -e "${RED}❌ Error: Job name is required${NC}"
    echo "Usage: $0 JOB_NAME [--params JSON_PARAMS]"
    echo "Available jobs: setup-environment, simulate-data, data-pipeline"
    exit 1
fi

JOB_NAME="$1"
shift

# Parse remaining arguments
PARAMS=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --params)
            PARAMS="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 JOB_NAME [--params JSON_PARAMS]"
            echo "Available jobs: setup-environment, simulate-data, data-pipeline"
            exit 0
            ;;
        *)
            echo "Unknown option $1"
            exit 1
            ;;
    esac
done

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

# Function to run a job
run_job() {
    local job_name="$1"
    local job_id="$2"
    local params="$3"
    
    echo -e "${YELLOW}Starting job: $job_name (ID: $job_id)${NC}"
    
    # Build the command
    local cmd="databricks jobs run-now --job-id $job_id"
    
    # Add parameters if provided
    if [ -n "$params" ]; then
        cmd="$cmd --json '$params'"
    fi
    
    # Execute the command
    echo -e "${BLUE}Executing: $cmd${NC}"
    local result=$(eval $cmd)
    
    # Extract run ID
    local run_id=$(echo "$result" | jq -r '.run_id')
    
    if [ "$run_id" != "null" ] && [ -n "$run_id" ]; then
        echo -e "${GREEN}✅ Job started successfully!${NC}"
        echo -e "${GREEN}Run ID: $run_id${NC}"
        echo -e "${YELLOW}Monitor progress with: databricks runs get --run-id $run_id${NC}"
        echo -e "${YELLOW}View logs with: databricks runs get-output --run-id $run_id${NC}"
        
        # Save run ID for reference
        echo "$run_id" > ".databricks/job-ids/$job_name-latest-run.txt"
    else
        echo -e "${RED}❌ Failed to start job: $job_name${NC}"
        echo "Response: $result"
        exit 1
    fi
}

# Main execution
case "$JOB_NAME" in
    "setup-environment"|"simulate-data"|"data-pipeline")
        JOB_ID=$(get_job_id "$JOB_NAME")
        run_job "$JOB_NAME" "$JOB_ID" "$PARAMS"
        ;;
    *)
        echo -e "${RED}❌ Unknown job name: $JOB_NAME${NC}"
        echo "Available jobs: setup-environment, simulate-data, data-pipeline"
        exit 1
        ;;
esac
