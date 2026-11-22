# Manage Kafka Connect connectors from the JSON files in this folder.
# Usage:
#   .\register-connectors.ps1              -> create/update all connectors (mongo-cdc-db-source, mongo-cdc-sourcemongo-source)
#   .\register-connectors.ps1 apply NAME   -> create/update only this connector (NAME.json)
#   .\register-connectors.ps1 status       -> show connector + task state
#   .\register-connectors.ps1 delete NAME  -> delete one connector (its offsets stay in Kafka)
param(
    [string]$Action = "apply",
    [string]$Name
)

$connect = "http://localhost:8083"
# de_source_mongo -> topic de_source_mongo   |   sourceMongo_db (transaction DB) -> topic sourceMongo_db
$names = @("mongo-cdc-db-source", "mongo-cdc-sourcemongo-source")

function Show-Status {
    foreach ($n in (Invoke-RestMethod "$connect/connectors")) {
        $s = Invoke-RestMethod "$connect/connectors/$n/status"
        $tasks = ($s.tasks | ForEach-Object { $_.state }) -join ","
        "{0,-30} connector={1,-8} tasks={2}" -f $n, $s.connector.state, $tasks
        foreach ($t in $s.tasks) { if ($t.state -eq "FAILED") { "   ERROR: " + ($t.trace -split "`n")[0] } }
    }
}

switch ($Action) {
    "apply" {
        $selected = if ($Name) { @($Name) } else { $names }
        foreach ($n in $selected) {
            $file = Join-Path $PSScriptRoot "$n.json"
            # PUT .../config creates the connector or updates it if it already exists
            Invoke-RestMethod -Method Put -Uri "$connect/connectors/$n/config" -ContentType "application/json" -InFile $file | Out-Null
            "applied $n"
        }
        Start-Sleep -Seconds 5
        Show-Status
    }
    "status" { Show-Status }
    "delete" {
        if (-not $Name) { throw "Usage: .\register-connectors.ps1 delete <name>" }
        Invoke-RestMethod -Method Delete -Uri "$connect/connectors/$Name" | Out-Null
        "deleted $Name"
    }
    default { throw "Unknown action '$Action' (use apply | status | delete)" }
}
