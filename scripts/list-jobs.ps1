# List all Databricks jobs
# Usage: .\scripts\list-jobs.ps1

# Colors for output
$Red = "Red"
$Green = "Green"
$Yellow = "Yellow"
$Blue = "Blue"

Write-Host "Databricks Jobs Status" -ForegroundColor $Blue
Write-Host "================================"

# List all jobs from Databricks
Write-Host "`nAll Jobs in Workspace:" -ForegroundColor $Yellow
try {
    $allJobs = databricks jobs list | ConvertFrom-Json
    $allJobs.jobs | Select-Object job_id, @{Name='name';Expression={$_.settings.name}}, @{Name='max_concurrent_runs';Expression={$_.settings.max_concurrent_runs}}, @{Name='timeout_seconds';Expression={$_.settings.timeout_seconds}} | ConvertTo-Json -Depth 2
} catch {
    Write-Host "Error retrieving jobs from workspace: $_" -ForegroundColor $Red
}

# List deployed jobs from local files
Write-Host "`nDeployed Jobs (Local):" -ForegroundColor $Yellow
if (Test-Path ".databricks\job-ids") {
    $jobIdFiles = Get-ChildItem ".databricks\job-ids\*.txt" -ErrorAction SilentlyContinue
    
    if ($jobIdFiles) {
        foreach ($jobFile in $jobIdFiles) {
            $jobName = [System.IO.Path]::GetFileNameWithoutExtension($jobFile.Name)
            $jobId = Get-Content $jobFile
            Write-Host "✅ $jobName (ID: $jobId)" -ForegroundColor $Green
        }
    } else {
        Write-Host "No deployed jobs found" -ForegroundColor $Red
    }
} else {
    Write-Host "No deployed jobs found" -ForegroundColor $Red
}

Write-Host "`nUsage:" -ForegroundColor $Blue
Write-Host "  .\scripts\run.ps1 <job-name>     # Run a job"
Write-Host "  .\scripts\status.ps1 <job-name>  # Check job status"
Write-Host "  .\scripts\deploy.ps1             # Deploy all jobs"
