# Kafka tests

To delete from Bitnami Kafka instance:

```bash
./entrypoint.sh kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic txs
```
