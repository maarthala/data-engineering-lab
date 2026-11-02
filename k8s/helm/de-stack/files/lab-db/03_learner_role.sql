-- The members' PostgreSQL login: learner / learner — READ-ONLY, like a production database
-- that data engineers may query but never change:
--   * reads every table in shopflow and adventureworks (the shared OLTP sources)
--   * can't insert, update, delete or create anything (writes go to the member's lakehouse)
--   * can't open the other databases on this server (members' own, postgres) — the live shop
--     `shop` has its own login, reader.
-- Idempotent: also run against an existing stack (psql -U postgres -f …).

DO $$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'learner') THEN
    CREATE ROLE learner LOGIN PASSWORD 'learner' NOSUPERUSER NOCREATEDB NOCREATEROLE;
  END IF;
END $$;
-- every session starts read-only too (clear error message; the grants below are the real lock)
ALTER ROLE learner SET default_transaction_read_only = on;

-- platform databases: only their own service user (the owner) may connect
DO $$ DECLARE d text; BEGIN
  FOR d IN SELECT datname FROM pg_database
           WHERE NOT datistemplate AND datname NOT IN ('shopflow', 'adventureworks', 'shop') LOOP
    EXECUTE format('REVOKE CONNECT ON DATABASE %I FROM PUBLIC', d);
  END LOOP;
END $$;
GRANT CONNECT ON DATABASE shopflow TO learner;

\connect shopflow
REASSIGN OWNED BY learner TO postgres;        -- tables members made before it was read-only
REVOKE CREATE ON SCHEMA public FROM learner, PUBLIC;
DO $$ DECLARE s text; BEGIN
  FOR s IN SELECT nspname FROM pg_namespace
           WHERE nspname NOT LIKE 'pg\_%' AND nspname <> 'information_schema' LOOP
    EXECUTE format('GRANT USAGE ON SCHEMA %I TO learner', s);
    EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO learner', s);
    EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA %I GRANT SELECT ON TABLES TO learner', s);
  END LOOP;
END $$;

-- adventureworks may not exist yet (k8s loads it later — its loader runs this part again)
SELECT 'adventureworks' AS db WHERE EXISTS (SELECT FROM pg_database WHERE datname = 'adventureworks') \gset
\if :{?db}
GRANT CONNECT ON DATABASE adventureworks TO learner;
\connect adventureworks
REASSIGN OWNED BY learner TO postgres;
REVOKE CREATE ON SCHEMA public FROM learner, PUBLIC;
DO $$ DECLARE s text; BEGIN
  FOR s IN SELECT nspname FROM pg_namespace
           WHERE nspname NOT LIKE 'pg\_%' AND nspname <> 'information_schema' LOOP
    EXECUTE format('GRANT USAGE ON SCHEMA %I TO learner', s);
    EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO learner', s);
    EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA %I GRANT SELECT ON TABLES TO learner', s);
  END LOOP;
END $$;
\endif
