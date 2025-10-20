# Check status of Databricks jobs
# Usage: .\scripts\status.ps1 [JOB_NAME]

param(
    [string]$JobName = ""
)

# Colors for output
$Red = "Red"
$Green = "Green"
$Yellow = "Yellow"
$Blue = "Blue"

# Function to get job ID from file
function Get-JobId {
    param([string]$JobName)
    
    $jobIdFile = ".databricks\job-ids\$JobName.txt"
    
    if (Test-Path $jobIdFile) {
        return Get-Content $jobIdFile
    } else {
        Write-Host "❌ Job ID file not found: $jobIdFile" -ForegroundColor $Red
        Write-Host "Please run '.\scripts\deploy.ps1 -JobName $JobName' first"
        exit 1
    }
}

# Function to get latest run ID
function Get-LatestRunId {
    param([string]$JobName)
    
    $runIdFile = ".databricks\job-ids\$JobName-latest-run.txt"
    
    if (Test-Path $runIdFile) {
        return Get-Content $runIdFile
    } else {
        return ""
    }
}

# Function to check job status
function Get-JobStatus {
    param([string]$JobName)
    
    $jobId = Get-JobId $JobName
    
    Write-Host "Job: $JobName (ID: $jobId)" -ForegroundColor $Blue
    
    # Get job details
    $jobInfo = databricks jobs get --job-id $jobId | ConvertFrom-Json
    Write-Host "Job Details:" -ForegroundColor $Yellow
    $jobInfo.settings | Select-Object name, max_concurrent_runs, timeout_seconds, tags | ConvertTo-Json -Depth 3
    
    # Get latest runs
    Write-Host "`nRecent Runs:" -ForegroundColor $Yellow
    try {
        $runsOutput = databricks runs list --job-id $jobId --limit 5
        $runs = $runsOutput | ConvertFrom-Json
        if ($runs.runs) {
            $runs.runs | Select-Object run_id, @{Name='state';Expression={$_.state.life_cycle_state}}, start_time, end_time | ConvertTo-Json -Depth 2
        } else {
            Write-Host "No runs found for this job" -ForegroundColor $Yellow
        }
    } catch {
        Write-Host "Error retrieving runs: $_" -ForegroundColor $Red
    }
    
    # Get latest run details if available
    $latestRunId = Get-LatestRunId $JobName
    if ($latestRunId) {
        Write-Host "`nLatest Run Details (ID: $latestRunId):" -ForegroundColor $Yellow
        $runDetails = databricks runs get --run-id $latestRunId | ConvertFrom-Json
        $runDetails | Select-Object run_id, state, start_time, end_time, run_duration | ConvertTo-Json -Depth 2
    }
}

# Function to list all jobs
function Get-AllJobs {
    Write-Host "All Deployed Jobs:" -ForegroundColor $Blue
    
    $jobIdFiles = Get-ChildItem ".databricks\job-ids\*.txt" -ErrorAction SilentlyContinue
    
    if ($jobIdFiles) {
        foreach ($jobFile in $jobIdFiles) {
            $jobName = [System.IO.Path]::GetFileNameWithoutExtension($jobFile.Name)
            $jobId = Get-Content $jobFile
            
            Write-Host "`n--- $jobName ---" -ForegroundColor $Yellow
            Write-Host "Job ID: $jobId"
            
            # Get basic job info
            try {
                $jobInfo = databricks jobs get --job-id $jobId | ConvertFrom-Json
                Write-Host "Status: Active"
            } catch {
                Write-Host "Status: Not found or deleted" -ForegroundColor $Red
            }
        }
    } else {
        Write-Host "No deployed jobs found" -ForegroundColor $Red
    }
}

# Main execution
if ($JobName) {
    switch ($JobName) {
        "setup-environment" {
            Get-JobStatus "setup-environment"
        }
        "simulate-data" {
            Get-JobStatus "simulate-data"
        }
        "data-pipeline" {
            Get-JobStatus "data-pipeline"
        }
        default {
            Write-Host "❌ Unknown job name: $JobName" -ForegroundColor $Red
            Write-Host "Available jobs: setup-environment, simulate-data, data-pipeline"
            exit 1
        }
    }
} else {
    Get-AllJobs
}
