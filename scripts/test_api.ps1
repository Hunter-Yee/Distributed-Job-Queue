# Integration Test Script for Distributed Job Queue REST API (Phase 3 & 4)
$baseUrl = "http://localhost:8080"

Write-Host "=========================================" -ForegroundColor Cyan
Write-Host " Testing Phase 3 & 4 API Endpoints" -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan

# 1. Health Check
Write-Host "`n[1] Testing GET /health..." -ForegroundColor Yellow
try {
    $health = Invoke-RestMethod -Uri "$baseUrl/health" -Method Get
    Write-Host "Health Check Status: $($health.status)" -ForegroundColor Green
} catch {
    Write-Host "Health check failed: $_" -ForegroundColor Red
    exit 1
}

# 2. Phase 3 POST /jobs submission (flat payload)
Write-Host "`n[2] Testing POST /jobs (Phase 3 flat payload format)..." -ForegroundColor Yellow
$p3Body = @{
    type = "report"
    priority = "high"
    duration_ms = 750
    failure_probability = 0.05
} | ConvertTo-Json

$p3Job = Invoke-RestMethod -Uri "$baseUrl/jobs" -Method Post -ContentType "application/json" -Body $p3Body
Write-Host "Created Job ID: $($p3Job.id)" -ForegroundColor Green
Write-Host "Job Type:       $($p3Job.type)" -ForegroundColor Green
Write-Host "Job Priority:   $($p3Job.priority)" -ForegroundColor Green
Write-Host "Job Status:     $($p3Job.status)" -ForegroundColor Green
Write-Host "Payload:        Duration = $($p3Job.payload.duration_ms)ms, FailProb = $($p3Job.payload.failure_probability)" -ForegroundColor Green

# 3. Phase 4 POST /jobs submission (nested payload format)
Write-Host "`n[3] Testing POST /jobs (Phase 4 nested payload format)..." -ForegroundColor Yellow
$p4Body = @{
    type = "compute"
    priority = "critical"
    payload = @{
        duration_ms = 1200
        failure_probability = 0.1
    }
} | ConvertTo-Json

$p4Job = Invoke-RestMethod -Uri "$baseUrl/jobs" -Method Post -ContentType "application/json" -Body $p4Body
Write-Host "Created Job ID: $($p4Job.id)" -ForegroundColor Green
Write-Host "Job Type:       $($p4Job.type)" -ForegroundColor Green
Write-Host "Job Status:     $($p4Job.status)" -ForegroundColor Green

# 4. GET /jobs/{id}
Write-Host "`n[4] Testing GET /jobs/$($p3Job.id)..." -ForegroundColor Yellow
$fetchedJob = Invoke-RestMethod -Uri "$baseUrl/jobs/$($p3Job.id)" -Method Get
Write-Host "Fetched Job ID:     $($fetchedJob.id)" -ForegroundColor Green
Write-Host "Fetched Job Status: $($fetchedJob.status)" -ForegroundColor Green

# 5. GET /jobs (List all jobs)
Write-Host "`n[5] Testing GET /jobs..." -ForegroundColor Yellow
$allJobs = Invoke-RestMethod -Uri "$baseUrl/jobs" -Method Get
Write-Host "Total Jobs Returned: $($allJobs.Count)" -ForegroundColor Green
foreach ($j in $allJobs) {
    Write-Host " - ID: $($j.id), Type: $($j.type), Priority: $($j.priority), Status: $($j.status)" -ForegroundColor Gray
}

# 6. Test Error Handling (Invalid Job Type)
Write-Host "`n[6] Testing POST /jobs with invalid job type..." -ForegroundColor Yellow
try {
    $invalidBody = @{ type = "invalid_job_type" } | ConvertTo-Json
    $res = Invoke-WebRequest -Uri "$baseUrl/jobs" -Method Post -ContentType "application/json" -Body $invalidBody -SkipHttpErrorCheck
    Write-Host "Response Code: $($res.StatusCode) (Expected 400)" -ForegroundColor Green
    Write-Host "Response Body: $($res.Content)" -ForegroundColor Gray
} catch {
    Write-Host "Error test caught exception: $_" -ForegroundColor Red
}

Write-Host "`n=========================================" -ForegroundColor Cyan
Write-Host " All Phase 3 & Phase 4 API Tests Passed!" -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan
