<#
.SYNOPSIS
    Runs the Go producer (Phase 1 historical replay) in the foreground.
.DESCRIPTION
    Replays the Olist dataset from gold.fct_orders/fct_order_items onto Redpanda
    for the running server's consumers. SPEED_MULTIPLIER=2880 means one
    historical day is replayed every 30 wall seconds; a full loop (~774 days)
    takes about 6.5 hours. The producer loops forever with per-loop suffixes.
#>
[CmdletBinding()]
param(
    [double]$SpeedMultiplier = 2880
)
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
if (-not $env:DATABASE_URL) {
    $env:DATABASE_URL = "postgres://abi:abi@localhost:5432/abi"
}
if (-not $env:ABI_KAFKA_SEED_BROKERS) {
    $env:ABI_KAFKA_SEED_BROKERS = "localhost:29092"
}
$env:ABI_SPEED_MULTIPLIER = "$SpeedMultiplier"
Push-Location (Join-Path $root "api")
Write-Host "Replaying history at ${SpeedMultiplier}x onto Redpanda  (Ctrl+C to stop)"
go run ./cmd/producer
Pop-Location