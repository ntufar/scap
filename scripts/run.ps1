# Run Databricks jobs using Databricks CLI
# Usage: .\scripts\run.ps1 JOB_NAME [-Params JSON_PARAMS]

param(
    [Parameter(Mandatory=$true)]
    [string]$JobName,
    [string]$Params = ""
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

# Function to run a job
function Start-Job {
    param(
        [string]$JobName,
        [string]$JobId,
        [string]$Params
    )
    
    Write-Host "Starting job: $JobName (ID: $JobId)" -ForegroundColor $Yellow
    
    # Build the command
    $cmd = "databricks jobs run-now --job-id $JobId"
    
    # Add parameters if provided
    if ($Params) {
        $cmd += " --json '$Params'"
    }
    
    # Execute the command
    Write-Host "Executing: $cmd" -ForegroundColor $Blue
    $result = Invoke-Expression $cmd | ConvertFrom-Json
    
    # Extract run ID
    $runId = $result.run_id
    
    if ($runId -and $runId -ne "null") {
        Write-Host "✅ Job started successfully!" -ForegroundColor $Green
        Write-Host "Run ID: $runId" -ForegroundColor $Green
        Write-Host "Monitor progress with: databricks runs get --run-id $runId" -ForegroundColor $Yellow
        Write-Host "View logs with: databricks runs get-output --run-id $runId" -ForegroundColor $Yellow
        
        # Save run ID for reference
        $runId | Out-File -FilePath ".databricks\job-ids\$JobName-latest-run.txt" -Encoding UTF8
    } else {
        Write-Host "❌ Failed to start job: $JobName" -ForegroundColor $Red
        Write-Host "Response: $($result | ConvertTo-Json)"
        exit 1
    }
}

# Main execution
switch ($JobName) {
    "setup-environment" {
        $JobId = Get-JobId "setup-environment"
        Start-Job "setup-environment" $JobId $Params
    }
    "simulate-data" {
        $JobId = Get-JobId "simulate-data"
        Start-Job "simulate-data" $JobId $Params
    }
    "data-pipeline" {
        $JobId = Get-JobId "data-pipeline"
        Start-Job "data-pipeline" $JobId $Params
    }
    default {
        Write-Host "❌ Unknown job name: $JobName" -ForegroundColor $Red
        Write-Host "Available jobs: setup-environment, simulate-data, data-pipeline"
        exit 1
    }
}
