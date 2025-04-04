
CREATE ROLE "coinbase-stage-mainnet-1" LOGIN
  PASSWORD 'coinbase-stage-mainnet-1'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant "coinbase-stage-mainnet-1" to postgresuser;
CREATE
DATABASE "coinbase-stage-mainnet-1"
  WITH OWNER = "coinbase-stage-mainnet-1"
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

CREATE ROLE "coinbase-stage-mainnet-2" LOGIN
  PASSWORD 'coinbase-stage-mainnet-2'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant "coinbase-stage-mainnet-2" to postgresuser;
CREATE
DATABASE "coinbase-stage-mainnet-2"
  WITH OWNER = "coinbase-stage-mainnet-2"
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

CREATE ROLE "coinbase-stage-mainnet-3" LOGIN
  PASSWORD 'coinbase-stage-mainnet-3'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant "coinbase-stage-mainnet-3" to postgresuser;
CREATE
DATABASE "coinbase-stage-mainnet-3"
  WITH OWNER = "coinbase-stage-mainnet-3"
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

CREATE ROLE "stage-mainnet-1" LOGIN
  PASSWORD 'stage-mainnet-1'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant "stage-mainnet-1" to postgresuser;
CREATE
DATABASE "stage-mainnet-1"
  WITH OWNER = "stage-mainnet-1"
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

CREATE ROLE "stage-mainnet-2" LOGIN
  PASSWORD 'stage-mainnet-2'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant "stage-mainnet-2" to postgresuser;
CREATE
DATABASE "stage-mainnet-2"
  WITH OWNER = "stage-mainnet-2"
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

CREATE ROLE "stage-mainnet-3" LOGIN
  PASSWORD 'stage-mainnet-3'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant "stage-mainnet-3" to postgresuser;
CREATE
DATABASE "stage-mainnet-3"
  WITH OWNER = "stage-mainnet-3"
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

