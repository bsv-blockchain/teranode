# drop all tables for local postgres dbs
psql postgres://ubsv:ubsv@localhost:5432/ubsv -c "drop table if exists state; drop table if exists utxos; drop table if exists txmeta; drop table if exists blocks;"
psql postgres://ubsv:ubsv@localhost:5432/coinbase -c "drop table if exists spendable_utxos; drop table if exists coinbase_utxos; drop table if exists blocks;"
