
CREATE ROLE "coinbase-stage-teranet-1" LOGIN
  PASSWORD 'coinbase-stage-teranet-1'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant "coinbase-stage-teranet-1" to postgresuser;
CREATE
DATABASE "coinbase-stage-teranet-1"
  WITH OWNER = "coinbase-stage-teranet-1"
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

CREATE ROLE "coinbase-stage-teranet-2" LOGIN
  PASSWORD 'coinbase-stage-teranet-2'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant "coinbase-stage-teranet-2" to postgresuser;
CREATE
DATABASE "coinbase-stage-teranet-2"
  WITH OWNER = "coinbase-stage-teranet-2"
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

CREATE ROLE "coinbase-stage-teranet-3" LOGIN
  PASSWORD 'coinbase-stage-teranet-3'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant "coinbase-stage-teranet-3" to postgresuser;
CREATE
DATABASE "coinbase-stage-teranet-3"
  WITH OWNER = "coinbase-stage-teranet-3"
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

CREATE ROLE "stage-teranet-1" LOGIN
  PASSWORD 'stage-teranet-1'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant "stage-teranet-1" to postgresuser;
CREATE
DATABASE "stage-teranet-1"
  WITH OWNER = "stage-teranet-1"
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

CREATE ROLE "stage-teranet-2" LOGIN
  PASSWORD 'stage-teranet-2'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant "stage-teranet-2" to postgresuser;
CREATE
DATABASE "stage-teranet-2"
  WITH OWNER = "stage-teranet-2"
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

CREATE ROLE "stage-teranet-3" LOGIN
  PASSWORD 'stage-teranet-3'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant "stage-teranet-3" to postgresuser;
CREATE
DATABASE "stage-teranet-3"
  WITH OWNER = "stage-teranet-3"
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

