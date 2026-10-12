-- Postgres roles and databases for the local compose/test stacks.
-- Roles are deliberately NOSUPERUSER; Teranode owns its databases and needs no
-- superuser rights.
--
-- This file runs only when the postgres data directory is empty. A stack brought
-- up before the NOSUPERUSER change keeps its superuser roles on the existing
-- volume. Recreate the storage ('docker compose down -v' for named volumes, or
-- delete the bind-mounted host directory, e.g. 'data/postgres', for stacks that
-- bind-mount instead), or, as the postgres superuser, run
-- 'ALTER ROLE <role> NOSUPERUSER;' for each role below.

CREATE ROLE miner1 LOGIN
  PASSWORD 'miner1'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant miner1 to postgres;
CREATE
DATABASE teranode1
  WITH OWNER = miner1
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

CREATE ROLE miner2 LOGIN
  PASSWORD 'miner2'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant miner2 to postgres;
CREATE
DATABASE teranode2
  WITH OWNER = miner2
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

CREATE ROLE miner3 LOGIN
  PASSWORD 'miner3'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant miner3 to postgres;
CREATE
DATABASE teranode3
  WITH OWNER = miner3
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

CREATE ROLE coinbase LOGIN
  PASSWORD 'coinbase'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant coinbase to postgres;
CREATE
DATABASE coinbase
  WITH OWNER = coinbase
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

CREATE ROLE coinbase1 LOGIN
  PASSWORD 'coinbase1'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant coinbase1 to postgres;
CREATE
DATABASE coinbase1
  WITH OWNER = coinbase1
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

CREATE ROLE coinbase2 LOGIN
  PASSWORD 'coinbase2'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant coinbase2 to postgres;
CREATE
DATABASE coinbase2
  WITH OWNER = coinbase2
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

CREATE ROLE coinbase3 LOGIN
  PASSWORD 'coinbase3'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant coinbase3 to postgres;
CREATE
DATABASE coinbase3
  WITH OWNER = coinbase3
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;
