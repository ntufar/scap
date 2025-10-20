#!/bin/bash

# Delete Databricks jobs
# Usage: ./scripts/delete.sh JOB_NAME

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
    echo "Usage: $0 JOB_NAME"
    echo "Available jobs: setup-environment, simulate-data, data-pipeline"
    exit 1
fi

JOB_NAME="$1"

# Function to get job ID from file
get_job_id() {
    local job_name="$1"
    local job_id_file=".databricks/job-ids/$job_name.txt"
    
    if [ -f "$job_id_file" ]; then
        cat "$job_id_file"
    else
        echo -e "${RED}❌ Job ID file not found: $job_id_file${NC}"
        echo "Job may not be deployed or already deleted"
        exit 1
    fi
}

# Function to delete a job
delete_job() {
    local job_name="$1"
    local job_id=$(get_job_id "$job_name")
    
    echo -e "${YELLOW}Deleting job: $job_name (ID: $job_id)${NC}"
    
    # Confirm deletion
    read -p "Are you sure you want to delete job '$job_name'? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${YELLOW}Deletion cancelled${NC}"
        exit 0
    fi
    
    # Delete the job
    if databricks jobs delete --job-id "$job_id"; then
        echo -e "${GREEN}✅ Job '$job_name' deleted successfully${NC}"
        
        # Remove local job ID file
        rm -f ".databricks/job-ids/$job_name.txt"
        rm -f ".databricks/job-ids/$job_name-latest-run.txt"
        
        echo -e "${GREEN}✅ Local job files cleaned up${NC}"
    else
        echo -e "${RED}❌ Failed to delete job: $job_name${NC}"
        exit 1
    fi
}

# Main execution
case "$JOB_NAME" in
    "setup-environment"|"simulate-data"|"data-pipeline")
        delete_job "$JOB_NAME"
        ;;
    "all")
        echo -e "${YELLOW}Deleting all jobs...${NC}"
        for job_file in .databricks/job-ids/*.txt; do
            if [ -f "$job_file" ]; then
                local job_name=$(basename "$job_file" .txt)
                if [ "$job_name" != "setup-environment-latest-run" ] && [ "$job_name" != "simulate-data-latest-run" ] && [ "$job_name" != "data-pipeline-latest-run" ]; then
                    delete_job "$job_name"
                fi
            fi
        done
        ;;
    *)
        echo -e "${RED}❌ Unknown job name: $JOB_NAME${NC}"
        echo "Available jobs: setup-environment, simulate-data, data-pipeline, all"
        exit 1
        ;;
esac
