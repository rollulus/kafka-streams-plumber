Example: time
=============

Shows how to `filter` and `map` key values, and how to do iso8601 datetime conversion.

 - input: key = iso8601 `string`, value = number as `string`
 - output: key = unixmilis `long`, value = number as `long` -- if number can be parsed

Build Plumber. Make sure that Kafka and the Schema Registry are running. From the current directory:

 * Start a Plumber instance:

```bash
../../plumber.sh -i example-in \
                 -o example-out \
                 -p example.properties \
                 -l example.lua \
                 -t example.test.lua \
                 -d string,string \
                 -s long,long
```

This instructs Plumber to source from topic `example-in`, sink to `example-out`, use the servers configured in `example.properties`, execute the Plumber script `example.lua`, verify the working of the script through `example.test.lua`, deserialize incoming values of type `string,string` and serialize outgoing values as `long,long`.

 * In another terminal, start a Kafka Avro consumer (part of the Confluent platform):

```bash
kafka-console-consumer --zookeeper localhost:2181 \
                       --topic example-out \
                       --property print.key=true \
                       --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer \
                       --property key.deserializer=org.apache.kafka.common.serialization.LongDeserializer
```

 * And in yet another terminal, produce a message using the Kafka Avro consumer (part of the Confluent platform):

```bash
echo "\
2016-05-13T16:43:12.345+00:00,42\n\
2016-05-13T16:54:32.101+00:00,fourtytwo\n\
2016-11-22T11:33:22.444+00:00,4242"     | kafka-console-producer --broker-list localhost:9092 \
                                                               --topic example-in \
                                                               --property parse.key=true \
                                                               --property key.separator=,
```

If all went fine, the consumer should then output:

```
1463157792345   42
1479814402444   4242
```

with the unparsable "number" `fourtytwo` filtered out
