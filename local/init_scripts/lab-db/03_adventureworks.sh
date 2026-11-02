#!/bin/bash
# Create + load the AdventureWorks (OLTP) sample database.
#
# The data (fixed CSVs + install.sql) is staged into ./adventureworks by
# `make init` and mounted here at /docker-entrypoint-initdb.d/adventureworks.
# Runs once, on first Postgres init. Skips gracefully if the data isn't staged
# (e.g. `make init` wasn't run, or ruby was missing) so the stack still starts.
set -e

AW="/docker-entrypoint-initdb.d/adventureworks"

if [ ! -f "$AW/install.sql" ]; then
  echo "AdventureWorks data not staged at $AW — run 'make init' before 'make up' to enable it. Skipping."
  exit 0
fi

echo "Creating + loading the 'adventureworks' database ..."
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -c 'CREATE DATABASE adventureworks;'

# install.sql uses \copy FROM './*.csv' (relative), so run psql from the data dir.
cd "$AW"
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d adventureworks -f install.sql >/dev/null

# Make the dataset first-class for Trino/SQLPad: AdventureWorks declares money
# columns as *unbounded* numeric, which Trino can't map (it returns them as text,
# breaking SUM/arithmetic and BI aggregation). Bound them to numeric(38,6). The
# convenience/reporting views depend on those columns, so drop them first (they
# aren't needed for querying the base tables).
echo "Bounding numeric columns for Trino/SQLPad compatibility ..."
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d adventureworks <<'SQL' >/dev/null
DO $$ DECLARE r record; BEGIN
  FOR r IN SELECT table_schema, table_name FROM information_schema.views
           WHERE table_schema NOT IN ('pg_catalog','information_schema')
  LOOP EXECUTE format('DROP VIEW IF EXISTS %I.%I CASCADE', r.table_schema, r.table_name); END LOOP;
END $$;
DO $$ DECLARE r record; BEGIN
  FOR r IN SELECT table_schema, table_name, column_name FROM information_schema.columns
           WHERE data_type='numeric' AND numeric_precision IS NULL
             AND table_schema NOT IN ('pg_catalog','information_schema')
  LOOP EXECUTE format('ALTER TABLE %I.%I ALTER COLUMN %I TYPE numeric(38,6)', r.table_schema, r.table_name, r.column_name); END LOOP;
END $$;
SQL

echo "✅ AdventureWorks loaded (68 tables across person/sales/production/purchasing/humanresources)."
