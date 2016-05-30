Example: csv
============

Shows how to translate a bunch of `strings` containing csv records into a partial GitHub `User` structure Avro.

 - input: value = csv record `string`;
 - output: value = A partial GitHub user record with schema `example.avsc`.

E.g. in:

```
rollulus,2015-01-14T07:36:24Z,https://keybase.io/rollulus,8
```

 Out:

```json
{
  "login": "rollulus",
  "created_at": "2015-01-14T07:36:24Z",
  "blog": {
    "string": "https://keybase.io/rollulus"
  },
  "public_repos": 8
}
```

Build Plumber. Make sure that Kafka and the Schema Registry are running. From the current directory:

 * Start a Plumber instance:

```bash
../../plumber.sh -i example-in \
                 -o example-out \
                 -p example.properties \
                 -l example.lua \
                 -t example.test.lua \
                 -d string \
                 -s avro=example.avsc
```

This instructs Plumber to source from topic `example-in`, sink to `example-out`, use the servers configured in `example.properties`, execute the Plumber script `example.lua`, verify the working of the script through `example.test.lua`, deserialize incoming values of type `string` and serialize outgoing values as an Avro record with the schema in `example.avsc`.

 * In another terminal, start a Kafka Avro consumer (part of the Confluent platform):

```bash
kafka-avro-console-consumer --topic example-out \
                            --zookeeper localhost:2181
```

 * And in yet another terminal, produce a few messages using the Kafka consumer (part of the Confluent platform):

```bash
cat example.csv | kafka-console-producer --broker-list localhost:9092 \
                                         --topic example-in
```

(`example.csv` contains a csv representation of a subset of GitHub users)

If all went fine, the consumer should then output:

```json
{"login":"aandradaa","created_at":"2016-05-18T11:34:31Z","blog":null,"public_repos":0}
{"login":"andrewstevenson","created_at":"2014-12-18T18:29:00Z","blog":{"string":"http://www.datamountaineer.com/"},"public_repos":4}
{"login":"Chrizje","created_at":"2013-01-09T23:05:01Z","blog":null,"public_repos":1}
{"login":"dudebowski","created_at":"2012-09-03T11:33:46Z","blog":null,"public_repos":2}
{"login":"GodlyLubu","created_at":"2016-02-19T09:02:17Z","blog":null,"public_repos":0}
{"login":"rollulus","created_at":"2015-01-14T07:36:24Z","blog":{"string":"https://keybase.io/rollulus"},"public_repos":8}
```
