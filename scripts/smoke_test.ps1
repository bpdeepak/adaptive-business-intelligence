<#
.SYNOPSIS
    Starts producer + server (temporarily) and runs an end-to-end smoke test.
.DESCRIPTION
    Requires the full pipeline already built and running (docker compose
    up -d --wait, data downloaded, loader run, dbt build). Builds
    bin/server.exe + bin/producer.exe, starts them on scratch ports, then
    asserts:
      * the Phase 0 batch endpoints return the known-good numbers
      * the metrics catalog carries the Phase 1 realtime definitions (v1.1.0+)
      * the live SSE stream emits metrics frames
      * the realtime REST surface and the anomalies endpoint answer
    The processes are stopped afterwards.
#>
[CmdletBinding()]
param(
    [string]$BaseUrl = "http://localhost:18085",
    [string]$HttpAddr = ":18085",
    [string]$GrpcAddr = ":18090"
)
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent

if (-not $env:DATABASE_URL) {
    $env:DATABASE_URL = "postgres://abi:abi@localhost:5432/abi"
}
$env:ABI_HTTP_ADDR = $HttpAddr
$env:ABI_GRPC_ADDR = $GrpcAddr
if (-not $env:ABI_KAFKA_SEED_BROKERS) {
    $env:ABI_KAFKA_SEED_BROKERS = "localhost:29092"
}

# Build then run the compiled binaries: killing `go run` leaves orphaned child
# processes behind, so we run bin/*.exe directly.
$apiDir = Join-Path $root "api"
Push-Location $apiDir
go build -o bin/server.exe ./cmd/server
if ($LASTEXITCODE -ne 0) { Pop-Location; throw "server build failed" }
go build -o bin/producer.exe ./cmd/producer
if ($LASTEXITCODE -ne 0) { Pop-Location; throw "producer build failed" }
Pop-Location

Write-Host "==> Starting server (bin/server.exe)"
$server = Start-Process -FilePath (Join-Path $apiDir "bin\server.exe") -PassThru -WindowStyle Hidden

Write-Host "==> Starting producer (28800x -> 1 historical day ~= 3s)"
$env:ABI_SPEED_MULTIPLIER = "28800"
$producer = Start-Process -FilePath (Join-Path $apiDir "bin\producer.exe") -PassThru -WindowStyle Hidden
Remove-Item Env:ABI_SPEED_MULTIPLIER -ErrorAction SilentlyContinue

try {
    $healthy = $false
    for ($i = 0; $i -lt 40; $i++) {
        try {
            $health = Invoke-RestMethod "$BaseUrl/healthz" -TimeoutSec 3
            if ($health.data.status -eq "ok") { $healthy = $true; break }
        } catch { Start-Sleep -Seconds 1 }
    }
    if (-not $healthy) { throw "server did not become healthy at $BaseUrl" }
    Write-Host "  healthz ok"

    $buckets = @()
    for ($i = 0; $i -lt 45; $i++) {
        try {
            $rt = Invoke-RestMethod "$BaseUrl/api/v1/realtime/metrics?limit=60" -TimeoutSec 3
            $buckets = @($rt.data.buckets)
            if ($buckets.Count -ge 1) { break }
        } catch { }
        Start-Sleep -Seconds 1
    }
    if ($buckets.Count -lt 1) { throw "no realtime buckets appeared" }
    Write-Host ("  realtime buckets ok  {0} buckets" -f $buckets.Count)

    $sseFile = Join-Path $env:TEMP ("abi_sse_{0}.txt" -f $PID)
    curl.exe -sN --max-time 12 "$BaseUrl/api/v1/stream/metrics" -o $sseFile
    $sse = Get-Content $sseFile -Raw -ErrorAction SilentlyContinue
    if ($sse -notmatch "event: metrics") { throw "no SSE metrics frame" }
    Write-Host "  sse ok"

    $summary = Invoke-RestMethod "$BaseUrl/api/v1/summary" -TimeoutSec 10
    if ($summary.data.revenue -le 0) { throw "summary.revenue not positive: $($summary.data.revenue)" }
    if ($summary.data.orders  -le 0) { throw "summary.orders not positive: $($summary.data.orders)" }
    Write-Host ("  summary ok  revenue={0:N2} orders={1} aov={2:N2} top={3}" -f `
        $summary.data.revenue, $summary.data.orders, $summary.data.aov, $summary.data.top_category)

    $revenue = Invoke-RestMethod "$BaseUrl/api/v1/revenue/daily" -TimeoutSec 10
    if (@($revenue.data).Count -lt 2) { throw "revenue/daily returned too few points" }
    Write-Host ("  revenue/daily ok  {0} points" -f @($revenue.data).Count)

    $orders = Invoke-RestMethod "$BaseUrl/api/v1/orders/daily" -TimeoutSec 10
    if (@($orders.data).Count -lt 2) { throw "orders/daily returned too few points" }
    Write-Host ("  orders/daily ok   {0} points" -f @($orders.data).Count)

    $cats = Invoke-RestMethod "$BaseUrl/api/v1/categories/top?metric=revenue&limit=5" -TimeoutSec 10
    if (@($cats.data).Count -eq 0) { throw "categories/top returned no categories" }
    Write-Host ("  categories/top ok top5: {0}" -f (($cats.data | ForEach-Object { $_.category }) -join ", "))

    $metrics = Invoke-RestMethod "$BaseUrl/api/v1/metrics" -TimeoutSec 10
    if (@($metrics.data.metrics).Count -lt 11) { throw "metrics catalog too small" }
    $names = @($metrics.data.metrics | ForEach-Object { $_.name })
    foreach ($need in @("revenue", "orders", "revenue_realtime", "orders_realtime", "active_sessions")) {
        if ($names -notcontains $need) { throw "metrics catalog missing $need" }
    }
    Write-Host ("  metrics ok  {0} definitions (v{1})" -f @($metrics.data.metrics).Count, $metrics.data.version)

    $rt2 = Invoke-RestMethod "$BaseUrl/api/v1/realtime/metrics?limit=5" -TimeoutSec 10
    $b2 = @($rt2.data.buckets)
    if ($b2.Count -lt 1) { throw "realtime metrics empty" }
    $last = $b2[-1]
    if (-not $last.bucket_start -or $null -eq $last.revenue -or $null -eq $last.orders) { throw "bucket shape wrong" }
    Write-Host ("  realtime metrics ok  latest {0} revenue={1:N2} orders={2}" -f `
        $last.bucket_start, $last.revenue, $last.orders)

    $anoms = Invoke-RestMethod "$BaseUrl/api/v1/anomalies" -TimeoutSec 10
    Write-Host ("  anomalies ok  {0} open" -f @($anoms.data).Count)

    Write-Host ""
    Write-Host "SMOKE TEST PASSED"
} finally {
    if ($producer -and -not $producer.HasExited) { Stop-Process -Id $producer.Id -Force }
    if ($server  -and -not $server.HasExited)  { Stop-Process -Id $server.Id -Force }
    Remove-Item Env:ABI_HTTP_ADDR, Env:ABI_GRPC_ADDR -ErrorAction SilentlyContinue
    Write-Host "Server + producer stopped."
}