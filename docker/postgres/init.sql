CREATE DATABASE mlflow;
CREATE DATABASE superset;
CREATE DATABASE pm;
\connect pm

CREATE TABLE engines (
  engine_id      TEXT PRIMARY KEY,
  model          TEXT NOT NULL,
  site           TEXT NOT NULL,
  operator_name  TEXT,
  operator_email TEXT,
  status         TEXT NOT NULL DEFAULT 'RUNNING' CHECK (status IN ('RUNNING','FAILED','MAINTENANCE')),
  failure_cycle  INT,
  created_at     TIMESTAMPTZ DEFAULT now()
);

CREATE SCHEMA analytics;
CREATE ROLE bi_reader LOGIN PASSWORD 'bi_reader';
GRANT CONNECT ON DATABASE pm TO bi_reader;
GRANT USAGE ON SCHEMA analytics TO bi_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT SELECT ON TABLES TO bi_reader;