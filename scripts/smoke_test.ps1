<#
.SYNOPSIS
    Starts the API (temporarily) and runs an end-to-end smoke test against it.
.DESCRIPTION
    Polls /healthz, then verifies /api/v1/summary, /revenue/daily, /orders/daily
    and /categories/top return sane data. The API process is stopped afterwards.
#>
[CmdletBinding()]
param(
    [string]$BaseUrl = "http://localhost:8080",
    [string]$HttpAddr = ":8080"
)
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent

if (-not $env:DATABASE_URL) {
    $env:DATABASE_URL = "postgres://abi:abi@localhost:5432/abi"
}
$env:ABI_HTTP_ADDR = $HttpAddr

# Build then run the compiled binary: killing `go run` leaves an orphaned
# child api.exe behind, so we run bin/api.exe directly instead.
$apiDir = Join-Path $root "api"
Push-Location $apiDir
go build -o bin/api.exe ./cmd/api
if ($LASTEXITCODE -ne 0) { Pop-Location; throw "go build failed" }
Pop-Location

Write-Host "==> Starting API (bin/api.exe)"
$proc = Start-Process -FilePath (Join-Path $apiDir "bin\api.exe") -PassThru -WindowStyle Hidden

try {
    $healthy = $false
    for ($i = 0; $i -lt 30; $i++) {
        try {
            $health = Invoke-RestMethod "$BaseUrl/healthz" -TimeoutSec 3
            if ($health.data.status -eq "ok") { $healthy = $true; break }
        } catch { Start-Sleep -Seconds 1 }
    }
    if (-not $healthy) { throw "API did not become healthy at $BaseUrl" }
    Write-Host "  healthz ok"

    $summary = Invoke-RestMethod "$BaseUrl/api/v1/summary" -TimeoutSec 10
    if ($summary.data.revenue -le 0)  { throw "summary.revenue not positive: $($summary.data.revenue)" }
    if ($summary.data.orders  -le 0)  { throw "summary.orders not positive: $($summary.data.orders)" }
    Write-Host ("  summary ok  revenue={0:N2} orders={1} aov={2:N2} top={3}" -f `
        $summary.data.revenue, $summary.data.orders, $summary.data.aov, $summary.data.top_category)

    $revenue = Invoke-RestMethod "$BaseUrl/api/v1/revenue/daily" -TimeoutSec 10
    if (@($revenue.data).Count -lt 2) { throw "revenue/daily returned too few points" }
    Write-Host ("  revenue/daily ok  {0} points" -f @($revenue.data).Count)

    $orders = Invoke-RestMethod "$BaseUrl/api/v1/orders/daily" -TimeoutSec 10
    if (@($orders.data).Count -lt 2)  { throw "orders/daily returned too few points" }
    Write-Host ("  orders/daily ok   {0} points" -f @($orders.data).Count)

    $cats = Invoke-RestMethod "$BaseUrl/api/v1/categories/top?metric=revenue&limit=5" -TimeoutSec 10
    if (@($cats.data).Count -eq 0)    { throw "categories/top returned no categories" }
    Write-Host ("  categories/top ok top5: {0}" -f (($cats.data | ForEach-Object { $_.category }) -join ", "))

    Write-Host ""
    Write-Host "SMOKE TEST PASSED"
} finally {
    if ($proc -and -not $proc.HasExited) { Stop-Process -Id $proc.Id -Force }
    Remove-Item Env:ABI_HTTP_ADDR -ErrorAction SilentlyContinue
    Write-Host "API stopped."
}