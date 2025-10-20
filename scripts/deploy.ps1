# Deploy jobs to Databricks using Databricks CLI
# Usage: .\scripts\deploy.ps1 [-JobName JOB_NAME] [-Params JSON_PARAMS]

param(
    [string]$JobName = "",
    [string]$Params = ""
)

# Colors for output
$Red = "Red"
$Green = "Green"
$Yellow = "Yellow"
$Blue = "Blue"

# Function to deploy a single job
function Deploy-Job {
    param(
        [string]$JobFile,
        [string]$JobName
    )
    
    Write-Host "Deploying job: $JobName" -ForegroundColor $Yellow
    
    # Create job and capture job ID
    $result = databricks jobs create --json-file $JobFile | ConvertFrom-Json
    $jobId = $result.job_id
    
    if ($jobId -and $jobId -ne "null") {
        Write-Host "✅ Job '$JobName' created successfully with ID: $jobId" -ForegroundColor $Green
        
        # Save job ID to file
        $jobIdFile = ".databricks\job-ids\$JobName.txt"
        $jobId | Out-File -FilePath $jobIdFile -Encoding UTF8
        
        # Update job if it already exists
        if (Test-Path $jobIdFile) {
            $existingId = Get-Content $jobIdFile
            if ($existingId -ne $jobId) {
                Write-Host "Updating existing job ID: $existingId -> $jobId" -ForegroundColor $Yellow
                $jobId | Out-File -FilePath $jobIdFile -Encoding UTF8
            }
        }
    } else {
        Write-Host "❌ Failed to create job: $JobName" -ForegroundColor $Red
        return $false
    }
    return $true
}

# Function to deploy all jobs
function Deploy-All {
    Write-Host "Deploying all jobs..." -ForegroundColor $Yellow
    
    # Deploy setup environment job
    Deploy-Job ".databricks\jobs\setup-environment.json" "setup-environment"
    
    # Deploy simulate data job
    Deploy-Job ".databricks\jobs\simulate-data.json" "simulate-data"
    
    # Deploy data pipeline job
    Deploy-Job ".databricks\jobs\data-pipeline.json" "data-pipeline"
    
    Write-Host "✅ All jobs deployed successfully!" -ForegroundColor $Green
}

# Main deployment logic
if ($JobName) {
    switch ($JobName) {
        "setup-environment" {
            Deploy-Job ".databricks\jobs\setup-environment.json" "setup-environment"
        }
        "simulate-data" {
            Deploy-Job ".databricks\jobs\simulate-data.json" "simulate-data"
        }
        "data-pipeline" {
            Deploy-Job ".databricks\jobs\data-pipeline.json" "data-pipeline"
        }
        default {
            Write-Host "❌ Unknown job name: $JobName" -ForegroundColor $Red
            Write-Host "Available jobs: setup-environment, simulate-data, data-pipeline"
            exit 1
        }
    }
} else {
    Deploy-All
}

Write-Host "🎉 Deployment completed!" -ForegroundColor $Green
