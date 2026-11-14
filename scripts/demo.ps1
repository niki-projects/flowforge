param([string]$BaseUrl='http://localhost:8080', [string]$ApiKey=$env:FLOWFORGE_API_KEY)
$ErrorActionPreference='Stop'
if (-not $ApiKey) { $ApiKey='flowforge-local-development-key' }
$headers=@{'X-API-Key'=$ApiKey}
$workflow=Invoke-RestMethod -Method Post -Uri "$BaseUrl/api/v1/workflows" -Headers $headers -ContentType 'application/json' -Body (Get-Content "$PSScriptRoot/../samples/customer-workflow.json" -Raw)
$execution=Invoke-RestMethod -Method Post -Uri "$BaseUrl/api/v1/workflows/$($workflow.id)/executions" -Headers $headers -ContentType 'application/json' -Body '{"input":{"name":"Ada","email":"ada@example.com"}}'
$deadline=(Get-Date).AddSeconds(60)
do {
    $status=Invoke-RestMethod -Uri "$BaseUrl/api/v1/executions/$($execution.id)" -Headers $headers
    if ($status.status -in @('COMPLETED','FAILED','CANCELED')) { break }
    Start-Sleep -Milliseconds 300
} while ((Get-Date) -lt $deadline)
$status | ConvertTo-Json -Depth 20
if ($status.status -ne 'COMPLETED') { throw "Execution did not complete: $($status.status)" }
