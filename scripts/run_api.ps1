<#
.SYNOPSIS
    Runs the Go API in the foreground for local dev against dockerized Postgres.
#>
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
if (-not $env:DATABASE_URL) {
    $env:DATABASE_URL = "postgres://abi:abi@localhost:5432/abi"
}
Push-Location (Join-Path $root "api")
Write-Host "Serving dashboard + API at http://localhost:8080  (Ctrl+C to stop)"
go run ./cmd/api
Pop-Location