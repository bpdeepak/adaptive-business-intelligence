<#
.SYNOPSIS
    Runs the Go server (REST + SSE + gRPC + realtime consumers) in the
    foreground for local dev against dockerized Postgres + Redpanda.
.DESCRIPTION
    The server is the Phase 1 primary binary (cmd/server) and hosts the REST
    API, dashboard, SSE live stream, gRPC live metrics, the bronze-writer and
    the realtime aggregator. Start the producer separately with run_producer.ps1.
#>
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
if (-not $env:DATABASE_URL) {
    $env:DATABASE_URL = "postgres://abi:abi@localhost:5432/abi"
}
if (-not $env:ABI_KAFKA_SEED_BROKERS) {
    $env:ABI_KAFKA_SEED_BROKERS = "localhost:29092"
}
Push-Location (Join-Path $root "api")
Write-Host "Serving dashboard + API at http://localhost:8080, gRPC at :8090  (Ctrl+C to stop)"
go run ./cmd/server
Pop-Location