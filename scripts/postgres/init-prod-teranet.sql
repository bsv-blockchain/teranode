
CREATE ROLE "coinbase-prod-teranet-1" LOGIN
  PASSWORD 'coinbase-prod-teranet-1'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant "coinbase-prod-teranet-1" to postgresuser;
CREATE
DATABASE "coinbase-prod-teranet-1"
  WITH OWNER = "coinbase-prod-teranet-1"
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

CREATE ROLE "coinbase-prod-teranet-2" LOGIN
  PASSWORD 'coinbase-prod-teranet-2'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant "coinbase-prod-teranet-2" to postgresuser;
CREATE
DATABASE "coinbase-prod-teranet-2"
  WITH OWNER = "coinbase-prod-teranet-2"
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

CREATE ROLE "coinbase-prod-teranet-3" LOGIN
  PASSWORD 'coinbase-prod-teranet-3'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant "coinbase-prod-teranet-3" to postgresuser;
CREATE
DATABASE "coinbase-prod-teranet-3"
  WITH OWNER = "coinbase-prod-teranet-3"
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

CREATE ROLE "prod-teranet-1" LOGIN
  PASSWORD 'prod-teranet-1'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant "prod-teranet-1" to postgresuser;
CREATE
DATABASE "prod-teranet-1"
  WITH OWNER = "prod-teranet-1"
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

CREATE ROLE "prod-teranet-2" LOGIN
  PASSWORD 'prod-teranet-2'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant "prod-teranet-2" to postgresuser;
CREATE
DATABASE "prod-teranet-2"
  WITH OWNER = "prod-teranet-2"
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

CREATE ROLE "prod-teranet-3" LOGIN
  PASSWORD 'prod-teranet-3'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant "prod-teranet-3" to postgresuser;
CREATE
DATABASE "prod-teranet-3"
  WITH OWNER = "prod-teranet-3"
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

