
import os
import csv
import psycopg

CONN_STR = (
    "postgresql://neondb_owner:npg_dYrx0ETujbK3"
    "@ep-soft-bar-adtib534-pooler.c-2.us-east-1.aws.neon.tech/neondb"
    "?sslmode=require&channel_binding=require"
)

CSV_PATH = "csv/crocodile_species.csv"

DDL = """
CREATE SCHEMA IF NOT EXISTS "RAW";
DROP TABLE IF EXISTS "RAW"."crocodile_species";
CREATE TABLE "RAW"."crocodile_species" (
  "Observation ID" integer,
  "Common Name" text,
  "Scientific Name" text,
  "Family" text,
  "Genus" text,
  "Observed Length (m)" text,
  "Observed Weight (kg)" text,
  "Age Class" text,
  "Sex" text,
  "Date of Observation" text,
  "Country/Region" text,
  "Habitat Type" text,
  "Conservation Status" text,
  "Observer Name" text,
  "Notes" text
);
"""

COPY_STAGE_SQL = """
DROP TABLE IF EXISTS _croc_stage;
CREATE TEMP TABLE _croc_stage (LIKE "RAW"."crocodile_species" INCLUDING ALL);
"""

COPY_SQL = """
COPY _croc_stage
("Observation ID","Common Name","Scientific Name","Family","Genus",
 "Observed Length (m)","Observed Weight (kg)","Age Class","Sex",
 "Date of Observation","Country/Region","Habitat Type",
 "Conservation Status","Observer Name","Notes")
FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', QUOTE '\"');
"""

MERGE_SQL = """
INSERT INTO "RAW"."crocodile_species" (
  "Observation ID","Common Name","Scientific Name","Family","Genus",
  "Observed Length (m)","Observed Weight (kg)","Age Class","Sex",
  "Date of Observation","Country/Region","Habitat Type",
  "Conservation Status","Observer Name","Notes"
)
SELECT
  "Observation ID","Common Name","Scientific Name","Family","Genus",
  "Observed Length (m)","Observed Weight (kg)","Age Class","Sex",
  "Date of Observation","Country/Region","Habitat Type",
  "Conservation Status","Observer Name","Notes"
FROM _croc_stage;
"""

INSERT_SQL = """
INSERT INTO "RAW"."crocodile_species" (
  "Observation ID","Common Name","Scientific Name","Family","Genus",
  "Observed Length (m)","Observed Weight (kg)","Age Class","Sex",
  "Date of Observation","Country/Region","Habitat Type",
  "Conservation Status","Observer Name","Notes"
) VALUES (
  %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
);
"""

def main():
    for v in ("PGHOST","PGPORT","PGDATABASE","PGUSER","PGPASSWORD","PGSERVICE"):
        os.environ.pop(v, None)

    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV not found: {CSV_PATH}")

    size = os.path.getsize(CSV_PATH)
    with open(CSV_PATH, "r", encoding="utf-8", newline="") as f:
        lines = sum(1 for _ in f)
    print(f"📄 CSV: {os.path.abspath(CSV_PATH)} | size={size} bytes | lines={lines}")

    print("Connecting to Neon…")
    with psycopg.connect(CONN_STR) as conn, conn.cursor() as cur:
        cur.execute("select current_database(), current_user")
        print("DB/User:", cur.fetchone())
        cur.execute("select version()")
        print("Server:", cur.fetchone()[0].splitlines()[0])

        print("Drop and recreate RAW.crocodile_species…")
        cur.execute(DDL)

        print("COPY fast path…")
        cur.execute(COPY_STAGE_SQL)
        with open(CSV_PATH, "r", encoding="utf-8", newline="") as f:
            cur.copy(COPY_SQL, f)
        cur.execute("SELECT COUNT(*) FROM _croc_stage")
        staged = cur.fetchone()[0]
        print("Staged via COPY:", staged)

        if staged > 0:
            cur.execute(MERGE_SQL)
        else:
            print("COPY staged 0 rows; falling back to executemany INSERT …")
            rows = []
            with open(CSV_PATH, "r", encoding="utf-8", newline="") as f:
                r = csv.DictReader(f)
                for d in r:
                    rows.append((
                        int(d["Observation ID"]) if d["Observation ID"] else None,
                        d["Common Name"],
                        d["Scientific Name"],
                        d["Family"],
                        d["Genus"],
                        d["Observed Length (m)"],
                        d["Observed Weight (kg)"],
                        d["Age Class"],
                        d["Sex"],
                        d["Date of Observation"],
                        d["Country/Region"],
                        d["Habitat Type"],
                        d["Conservation Status"],
                        d["Observer Name"],
                        d["Notes"],
                    ))
            if rows:
                cur.executemany(INSERT_SQL, rows)
                print(f"Inserted via executemany: {len(rows)}")
            else:
                print("No rows parsed from CSV.")

        cur.execute('SELECT COUNT(*) FROM "RAW"."crocodile_species"')
        total = cur.fetchone()[0]
        conn.commit()

    print(f"Done. RAW.crocodile_species rows: {total}")

if __name__ == "__main__":
    main()