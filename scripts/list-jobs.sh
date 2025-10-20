#!/bin/bash

# List all Databricks jobs
# Usage: ./scripts/list-jobs.sh

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}📋 Databricks Jobs Status${NC}"
echo "================================"

# List all jobs from Databricks
echo -e "\n${YELLOW}All Jobs in Workspace:${NC}"
databricks jobs list | jq '.jobs[] | {job_id, settings: {name: .settings.name, max_concurrent_runs: .settings.max_concurrent_runs, timeout_seconds: .settings.timeout_seconds}}'

# List deployed jobs from local files
echo -e "\n${YELLOW}Deployed Jobs (Local):${NC}"
if [ -d ".databricks/job-ids" ]; then
    for job_file in .databricks/job-ids/*.txt; do
        if [ -f "$job_file" ]; then
            local job_name=$(basename "$job_file" .txt)
            local job_id=$(cat "$job_file")
            echo -e "${GREEN}✅ $job_name${NC} (ID: $job_id)"
        fi
    done
else
    echo -e "${RED}No deployed jobs found${NC}"
fi

echo -e "\n${BLUE}💡 Usage:${NC}"
echo "  ./scripts/run.sh <job-name>     # Run a job"
echo "  ./scripts/status.sh <job-name>  # Check job status"
echo "  ./scripts/deploy.sh             # Deploy all jobs"
