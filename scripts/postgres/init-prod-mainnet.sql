
CREATE ROLE "coinbase-prod-eks-1-mainnet-1" LOGIN
  PASSWORD 'coinbase-prod-eks-1-mainnet-1'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant "coinbase-prod-eks-1-mainnet-1" to postgresuser;
CREATE
DATABASE "coinbase-prod-eks-1-mainnet-1"
  WITH OWNER = "coinbase-prod-eks-1-mainnet-1"
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

CREATE ROLE "coinbase-prod-eks-1-mainnet-2" LOGIN
  PASSWORD 'coinbase-prod-eks-1-mainnet-2'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant "coinbase-prod-eks-1-mainnet-2" to postgresuser;
CREATE
DATABASE "coinbase-prod-eks-1-mainnet-2"
  WITH OWNER = "coinbase-prod-eks-1-mainnet-2"
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

CREATE ROLE "coinbase-prod-eks-1-mainnet-3" LOGIN
  PASSWORD 'coinbase-prod-eks-1-mainnet-3'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant "coinbase-prod-eks-1-mainnet-3" to postgresuser;
CREATE
DATABASE "coinbase-prod-eks-1-mainnet-3"
  WITH OWNER = "coinbase-prod-eks-1-mainnet-3"
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

CREATE ROLE "prod-eks-1-mainnet-1" LOGIN
  PASSWORD 'prod-eks-1-mainnet-1'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant "prod-eks-1-mainnet-1" to postgresuser;
CREATE
DATABASE "prod-eks-1-mainnet-1"
  WITH OWNER = "prod-eks-1-mainnet-1"
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

CREATE ROLE "prod-eks-1-mainnet-2" LOGIN
  PASSWORD 'prod-eks-1-mainnet-2'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant "prod-eks-1-mainnet-2" to postgresuser;
CREATE
DATABASE "prod-eks-1-mainnet-2"
  WITH OWNER = "prod-eks-1-mainnet-2"
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

CREATE ROLE "prod-eks-1-mainnet-3" LOGIN
  PASSWORD 'prod-eks-1-mainnet-3'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
grant "prod-eks-1-mainnet-3" to postgresuser;
CREATE
DATABASE "prod-eks-1-mainnet-3"
  WITH OWNER = "prod-eks-1-mainnet-3"
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1;

