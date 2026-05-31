$ErrorActionPreference = "Stop"

$connectorFile = Join-Path $PSScriptRoot "..\debezium\postgres-connector.json"
$connector = Get-Content $connectorFile -Raw | ConvertFrom-Json
$connectorName = $connector.name

try {
    Invoke-RestMethod `
        -Method Post `
        -Uri "http://localhost:8083/connectors" `
        -ContentType "application/json" `
        -InFile $connectorFile

    Write-Host "Created Debezium connector: $connectorName"
}
catch {
    if ($null -eq $_.Exception.Response) {
        throw
    }

    $statusCode = $_.Exception.Response.StatusCode.value__

    if ($statusCode -ne 409) {
        throw
    }

    $config = $connector.config | ConvertTo-Json -Depth 20
    Invoke-RestMethod `
        -Method Put `
        -Uri "http://localhost:8083/connectors/$connectorName/config" `
        -ContentType "application/json" `
        -Body $config

    Write-Host "Updated Debezium connector: $connectorName"
}
