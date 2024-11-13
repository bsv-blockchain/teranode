
CREATE ROLE "staging-coinbase1" LOGIN
  PASSWORD 'staging-coinbase1'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant "staging-coinbase1" to postgresuser;
CREATE
DATABASE "staging-coinbase1"
  WITH OWNER = "staging-coinbase1"
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

CREATE ROLE "staging-coinbase2" LOGIN
  PASSWORD 'staging-coinbase2'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant "staging-coinbase2" to postgresuser;
CREATE
DATABASE "staging-coinbase2"
  WITH OWNER = "staging-coinbase2"
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

CREATE ROLE "staging-coinbase3" LOGIN
  PASSWORD 'staging-coinbase3'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant "staging-coinbase3" to postgresuser;
CREATE
DATABASE "staging-coinbase3"
  WITH OWNER = "staging-coinbase3"
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

CREATE ROLE "staging-teranet-1" LOGIN
  PASSWORD 'staging-teranet-1'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant "staging-teranet-1" to postgresuser;
CREATE
DATABASE "staging-teranet-1"
  WITH OWNER = "staging-teranet-1"
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

CREATE ROLE "staging-teranet-2" LOGIN
  PASSWORD 'staging-teranet-2'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant "staging-teranet-2" to postgresuser;
CREATE
DATABASE "staging-teranet-2"
  WITH OWNER = "staging-teranet-2"
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

CREATE ROLE "staging-teranet-3" LOGIN
  PASSWORD 'staging-teranet-3'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant "staging-teranet-3" to postgresuser;
CREATE
DATABASE "staging-teranet-3"
  WITH OWNER = "staging-teranet-3"
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

