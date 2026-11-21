#!/usr/bin/env bash
# Imports every sample collection into MongoDB with mongoimport (drops the collection first).
#   ./scripts/import-sample-data.sh mongodb://localhost:27017 bronze_db
set -euo pipefail
MONGO_URI="${1:-mongodb://localhost:27017}"
DATABASE="${2:-bronze_db}"
DATA_DIR="$(dirname "$0")/../src/main/resources/sample-data"

for file in "$DATA_DIR"/*.json; do
  collection="$(basename "$file" .json)"
  echo "Importing $collection ..."
  mongoimport --uri "$MONGO_URI" --db "$DATABASE" --collection "$collection" --jsonArray --drop --file "$file"
done
