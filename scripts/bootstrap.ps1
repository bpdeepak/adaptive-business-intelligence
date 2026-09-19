<#
.SYNOPSIS
    Phase 0 bootstrap: infra up -> download data -> load bronze -> dbt build -> build API.
.DESCRIPTION
    Runs the full Phase 0 pipeline end to end. Requires Docker Desktop running,
    Go on PATH, and the uv-managed Python environment synced (uv sync).
.EXAMPLE
    ./scripts/bootstrap.ps1
#>
[CmdletBinding()]
param(
    [switch]$SkipInfra,
    [switch]$SkipData,
    [switch]$SkipLoad
)
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
Set-Location $root

if (-not $SkipInfra) {
    Write-Host "==> Starting infrastructure (Postgres, MinIO, Redpanda)"
    docker compose up -d --wait
    if ($LASTEXITCODE -ne 0) { throw "docker compose up failed" }
}

if (-not $SkipData) {
    Write-Host "==> Downloading Olist dataset (public mirror)"
    uv run python scripts/download_olist.py
    if ($LASTEXITCODE -ne 0) { throw "dataset download failed" }
}

if (-not $SkipLoad) {
    Write-Host "==> Loading raw data into MinIO + Postgres bronze"
    uv run python loader/loader.py
    if ($LASTEXITCODE -ne 0) { throw "bronze load failed" }
}

Write-Host "==> Running dbt (silver + gold models + tests)"
uv run dbt build --project-dir dbt --profiles-dir dbt
if ($LASTEXITCODE -ne 0) { throw "dbt build failed" }

Write-Host "==> Generating dbt docs"
uv run dbt docs generate --project-dir dbt --profiles-dir dbt
if ($LASTEXITCODE -ne 0) { throw "dbt docs failed" }

Write-Host "==> Building Go binaries (server + producer)"
Push-Location api
go build -o bin/server.exe ./cmd/server
if ($LASTEXITCODE -ne 0) { Pop-Location; throw "server build failed" }
go build -o bin/producer.exe ./cmd/producer
if ($LASTEXITCODE -ne 0) { Pop-Location; throw "producer build failed" }
Pop-Location

Write-Host ""
Write-Host "Phase 1 pipeline ready."
Write-Host "  Dashboard/API : scripts/run_api.ps1  -> http://localhost:8080 (SSE live + gRPC :8090)"
Write-Host "  Replay        : scripts/run_producer.ps1  (SPEED_MULTIPLIER=2880 -> 1 day ~= 30s)"
Write-Host "  Smoke test    : scripts/smoke_test.ps1"
Write-Host "  MinIO console : http://localhost:9001  (minioadmin / minioadmin)"
Write-Host "  dbt docs      : dbt/target/index.html"