
CREATE ROLE "coinbase-stage-eks-2-teranet-1" LOGIN
  PASSWORD 'coinbase-stage-eks-2-teranet-1'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant "coinbase-stage-eks-2-teranet-1" to postgresuser;
CREATE
DATABASE "coinbase-stage-eks-2-teranet-1"
  WITH OWNER = "coinbase-stage-eks-2-teranet-1"
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

CREATE ROLE "coinbase-stage-eks-2-teranet-2" LOGIN
  PASSWORD 'coinbase-stage-eks-2-teranet-2'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant "coinbase-stage-eks-2-teranet-2" to postgresuser;
CREATE
DATABASE "coinbase-stage-eks-2-teranet-2"
  WITH OWNER = "coinbase-stage-eks-2-teranet-2"
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

CREATE ROLE "coinbase-stage-eks-2-teranet-3" LOGIN
  PASSWORD 'coinbase-stage-eks-2-teranet-3'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant "coinbase-stage-eks-2-teranet-3" to postgresuser;
CREATE
DATABASE "coinbase-stage-eks-2-teranet-3"
  WITH OWNER = "coinbase-stage-eks-2-teranet-3"
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

CREATE ROLE "stage-eks-2-teranet-1" LOGIN
  PASSWORD 'stage-eks-2-teranet-1'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant "stage-eks-2-teranet-1" to postgresuser;
CREATE
DATABASE "stage-eks-2-teranet-1"
  WITH OWNER = "stage-eks-2-teranet-1"
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

CREATE ROLE "stage-eks-2-teranet-2" LOGIN
  PASSWORD 'stage-eks-2-teranet-2'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant "stage-eks-2-teranet-2" to postgresuser;
CREATE
DATABASE "stage-eks-2-teranet-2"
  WITH OWNER = "stage-eks-2-teranet-2"
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

CREATE ROLE "stage-eks-2-teranet-3" LOGIN
  PASSWORD 'stage-eks-2-teranet-3'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant "stage-eks-2-teranet-3" to postgresuser;
CREATE
DATABASE "stage-eks-2-teranet-3"
  WITH OWNER = "stage-eks-2-teranet-3"
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

