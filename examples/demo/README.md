Example: demo
=============

The example in the main README, but less brief. Shows how to transform input Avro structures into some new form; shows how to test.

Build Plumber. Make sure that Kafka and the Schema Registry are running. From the current directory:

1. Start a Plumber instance:

```bash
../../plumber.sh -i example-in -o example-out -p example.properties -l example.lua -t example.test.lua -d avro -s avro=example.desired.avsc
```

This instructs Plumber to source from topic `example-in`, sink to `example-out`, use the servers configured in `example.properties`, execute the Plumber script `example.lua`, verify the working of the script through `example.test.lua`, deserialize incoming values of type avro (the schema is provided through the registry) and serialize outgoing values as avro with schema `example.desired.avsc`.

2. In another terminal, start a Kafka Avro consumer (part of the Confluent platform):

```bash
kafka-avro-console-consumer --topic example-out --zookeeper localhost:2181
```

3. And in yet another terminal, produce a message using the Kafka Avro consumer (part of the Confluent platform):

```bash
cat example.input.json | kafka-avro-console-producer --broker-list localhost:9092 --topic example-in --property value.schema=`cat example.undesired.avsc`
```

If all went fine, the consumer should output then:

```json
	{"valid":true,"name":"roel","fingers":10}
```
