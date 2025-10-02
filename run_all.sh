
set -euo pipefail
cd "$(dirname "$0")"

export PGHOST="ep-soft-bar-adtib534-pooler.c-2.us-east-1.aws.neon.tech"
export PGDATABASE="neondb"
export PGUSER="neondb_owner"
export PGPASSWORD="YOUR_PASSWORD"
export PGSSLMODE="require"
export PGCHANNELBINDING="require"


# Use malloy-cli 
MALLOY_CMD="${MALLOY_CMD:-npx malloy-cli}"

echo "Running full pipeline..."

# 1) Python ETL
echo "Fetch CSV"
python3 scripts/Fetch_crocodile_csv.py

echo "Load CSV to Neon"
python3 scripts/Csv_neon.py

# 2) Malloy models 
echo "QA Checks"
$MALLOY_CMD run models/CHECKS/QA_checks.malloysql

echo "Staging"
$MALLOY_CMD run models/STAGING/Staging_run.malloysql

echo "Dim Dates"
$MALLOY_CMD run models/GOLDEN/Dim_dates.malloysql

echo "Dim Location"
$MALLOY_CMD run models/GOLDEN/Dim_location.malloysql

echo "Dim Observer"
$MALLOY_CMD run models/GOLDEN/Dim_observer.malloysql

echo "Dim Species"
$MALLOY_CMD run models/GOLDEN/Dim_species.malloysql

echo "Dim Conservation Status"
$MALLOY_CMD run models/GOLDEN/Dim_conservation_status.malloysql

echo "Fact Crocodile Observations"
$MALLOY_CMD run models/GOLDEN/fact_crocodile_obs.malloysql

echo "All steps completed successfully."
