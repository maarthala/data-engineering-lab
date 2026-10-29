#!/usr/bin/env bash
# The members' Airflow DAG processor's login airflow_parser (its tables: airflow_lab_guard.py grants
# them at each Airflow start). Runs once, on a fresh database; an existing one: run it in the postgres pod.
set -euo pipefail
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d postgres -v pw="$PG_AIRFLOW_PARSER_PASSWORD" <<'SQL'
SELECT 'CREATE ROLE airflow_parser LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE'
 WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'airflow_parser') \gexec
ALTER ROLE airflow_parser PASSWORD :'pw';
GRANT CONNECT ON DATABASE airflow TO airflow_parser;
SQL
