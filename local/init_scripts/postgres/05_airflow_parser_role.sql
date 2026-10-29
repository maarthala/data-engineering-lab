-- The members' Airflow DAG processor's login: airflow_parser. Members' DAG files run there (top-level
-- code, DAG callbacks), so it gets Airflow's tables minus logins/permissions, connections, variables,
-- other runs' XComs … — airflow_lab_guard.py grants the tables (after each Airflow start; a migration
-- may add some). Idempotent: also run against an existing stack (psql -U postgres -f …).
DO $$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'airflow_parser') THEN
    CREATE ROLE airflow_parser LOGIN PASSWORD 'airflow_parser' NOSUPERUSER NOCREATEDB NOCREATEROLE;
  END IF;
END $$;
GRANT CONNECT ON DATABASE airflow TO airflow_parser;
