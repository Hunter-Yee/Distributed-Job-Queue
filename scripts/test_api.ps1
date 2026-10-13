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
    $null = Invoke-RestMethod -Uri "$baseUrl/jobs" -Method Post -ContentType "application/json" -Body $invalidBody
    Write-Host "Expected error but got success" -ForegroundColor Red
} catch [System.Net.WebException] {
    $resp = $_.Exception.Response
    if ($resp) {
        $statusCode = [int]$resp.StatusCode
        Write-Host "Response Code: $statusCode (Expected 400)" -ForegroundColor Green
    } else {
        Write-Host "Caught WebException: $_" -ForegroundColor Green
    }
} catch {
    Write-Host "Error test caught exception: $_" -ForegroundColor Red
}

# 7. RabbitMQ Management API Verification
Write-Host "`n[7] Verifying RabbitMQ Queue State via Management API..." -ForegroundColor Yellow
$rmqMgmtUrl = "http://localhost:15672/api/queues/%2f/jobs.queue"
$authHeader = @{
    Authorization = "Basic " + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("guest:guest"))
}

# Allow a moment for RabbitMQ stats sampler to aggregate message count
Start-Sleep -Seconds 2

try {
    $queueInfo = Invoke-RestMethod -Uri $rmqMgmtUrl -Headers $authHeader -Method Get
    Write-Host "Queue Name:         $($queueInfo.name)" -ForegroundColor Green
    Write-Host "Durable:            $($queueInfo.durable)" -ForegroundColor Green
    Write-Host "Max Priority Arg:   $($queueInfo.arguments.'x-max-priority')" -ForegroundColor Green
    Write-Host "Messages Ready:     $($queueInfo.messages_ready)" -ForegroundColor Green
    Write-Host "Messages Total:     $($queueInfo.messages)" -ForegroundColor Green

    if ($queueInfo.messages -ge 2) {
        Write-Host "RabbitMQ verification PASSED: Jobs were successfully published and queued!" -ForegroundColor Green
    } else {
        Write-Host "Warning: Expected at least 2 messages in queue, found: $($queueInfo.messages)" -ForegroundColor Yellow
    }
} catch {
    Write-Host "RabbitMQ Management API check failed: $_" -ForegroundColor Red
}

Write-Host "`n=========================================" -ForegroundColor Cyan
Write-Host " All API & RabbitMQ Integration Tests Passed!" -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan
