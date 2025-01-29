
CREATE ROLE "coinbase-prod-mainnet-1" LOGIN
  PASSWORD 'coinbase-prod-mainnet-1'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant "coinbase-prod-mainnet-1" to postgresuser;
CREATE
DATABASE "coinbase-prod-mainnet-1"
  WITH OWNER = "coinbase-prod-mainnet-1"
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

CREATE ROLE "coinbase-prod-mainnet-2" LOGIN
  PASSWORD 'coinbase-prod-mainnet-2'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant "coinbase-prod-mainnet-2" to postgresuser;
CREATE
DATABASE "coinbase-prod-mainnet-2"
  WITH OWNER = "coinbase-prod-mainnet-2"
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

CREATE ROLE "coinbase-prod-mainnet-3" LOGIN
  PASSWORD 'coinbase-prod-mainnet-3'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant "coinbase-prod-mainnet-3" to postgresuser;
CREATE
DATABASE "coinbase-prod-mainnet-3"
  WITH OWNER = "coinbase-prod-mainnet-3"
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

CREATE ROLE "prod-mainnet-1" LOGIN
  PASSWORD 'prod-mainnet-1'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant "prod-mainnet-1" to postgresuser;
CREATE
DATABASE "prod-mainnet-1"
  WITH OWNER = "prod-mainnet-1"
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

CREATE ROLE "prod-mainnet-2" LOGIN
  PASSWORD 'prod-mainnet-2'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant "prod-mainnet-2" to postgresuser;
CREATE
DATABASE "prod-mainnet-2"
  WITH OWNER = "prod-mainnet-2"
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

CREATE ROLE "prod-mainnet-3" LOGIN
  PASSWORD 'prod-mainnet-3'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant "prod-mainnet-3" to postgresuser;
CREATE
DATABASE "prod-mainnet-3"
  WITH OWNER = "prod-mainnet-3"
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

