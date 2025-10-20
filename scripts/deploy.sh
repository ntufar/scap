#!/bin/bash

# Deploy jobs to Databricks using Databricks CLI
# Usage: ./scripts/deploy.sh [--job JOB_NAME] [--params JSON_PARAMS]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default values
JOB_NAME=""
PARAMS=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --job)
      JOB_NAME="$2"
      shift 2
      ;;
    --params)
      PARAMS="$2"
      shift 2
      ;;
    -h|--help)
      echo "Usage: $0 [--job JOB_NAME] [--params JSON_PARAMS]"
      echo "  --job: Deploy specific job (setup-environment, simulate-data, data-pipeline)"
      echo "  --params: JSON parameters for job configuration"
      exit 0
      ;;
    *)
      echo "Unknown option $1"
      exit 1
      ;;
  esac
done

# Function to deploy a single job
deploy_job() {
    local job_file="$1"
    local job_name="$2"
    
    echo -e "${YELLOW}Deploying job: $job_name${NC}"
    
    # Create job and capture job ID
    local job_id=$(databricks jobs create --json-file "$job_file" | jq -r '.job_id')
    
    if [ "$job_id" != "null" ] && [ -n "$job_id" ]; then
        echo -e "${GREEN}✅ Job '$job_name' created successfully with ID: $job_id${NC}"
        
        # Save job ID to file
        echo "$job_id" > ".databricks/job-ids/$job_name.txt"
        
        # Update job if it already exists
        if [ -f ".databricks/job-ids/$job_name.txt" ]; then
            local existing_id=$(cat ".databricks/job-ids/$job_name.txt")
            if [ "$existing_id" != "$job_id" ]; then
                echo -e "${YELLOW}Updating existing job ID: $existing_id -> $job_id${NC}"
                echo "$job_id" > ".databricks/job-ids/$job_name.txt"
            fi
        fi
    else
        echo -e "${RED}❌ Failed to create job: $job_name${NC}"
        return 1
    fi
}

# Function to deploy all jobs
deploy_all() {
    echo -e "${YELLOW}Deploying all jobs...${NC}"
    
    # Deploy setup environment job
    deploy_job ".databricks/jobs/setup-environment.json" "setup-environment"
    
    # Deploy simulate data job
    deploy_job ".databricks/jobs/simulate-data.json" "simulate-data"
    
    # Deploy data pipeline job
    deploy_job ".databricks/jobs/data-pipeline.json" "data-pipeline"
    
    echo -e "${GREEN}✅ All jobs deployed successfully!${NC}"
}

# Main deployment logic
if [ -n "$JOB_NAME" ]; then
    case "$JOB_NAME" in
        "setup-environment")
            deploy_job ".databricks/jobs/setup-environment.json" "setup-environment"
            ;;
        "simulate-data")
            deploy_job ".databricks/jobs/simulate-data.json" "simulate-data"
            ;;
        "data-pipeline")
            deploy_job ".databricks/jobs/data-pipeline.json" "data-pipeline"
            ;;
        *)
            echo -e "${RED}❌ Unknown job name: $JOB_NAME${NC}"
            echo "Available jobs: setup-environment, simulate-data, data-pipeline"
            exit 1
            ;;
    esac
else
    deploy_all
fi

echo -e "${GREEN}🎉 Deployment completed!${NC}"
