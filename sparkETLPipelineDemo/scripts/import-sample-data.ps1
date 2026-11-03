# Imports every sample collection into MongoDB with mongoimport (drops the collection first).
# Not needed in the dev profile (the app loads the files itself), but handy for other environments.
#   .\scripts\import-sample-data.ps1 -MongoUri "mongodb://localhost:27017" -Database "bronze_db"
param(
    [string]$MongoUri = "mongodb://localhost:27017",
    [string]$Database = "bronze_db"
)

$dataDir = Join-Path $PSScriptRoot "..\src\main\resources\sample-data"
Get-ChildItem $dataDir -Filter *.json | ForEach-Object {
    $collection = $_.BaseName
    Write-Host "Importing $collection ..."
    mongoimport --uri $MongoUri --db $Database --collection $collection --jsonArray --drop --file $_.FullName
}
