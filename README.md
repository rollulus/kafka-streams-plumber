Kafka Streams Plumber
=====================

[![Build Status](https://travis-ci.org/rollulus/kafka-streams-plumber.svg?branch=master)](https://travis-ci.org/rollulus/kafka-streams-plumber)

Plumber is for the dirty work you do not want to do: silly transformations of your data structures because of slight mismatches. E.g. adding/removing fields, changing enums, et cetera. The transformation is described in Lua, you know, the language that scripts World of Warcraft, Redis and your wireless router at home for instance.

Proof of concept. Work in progress. Somewhere between a *horrible* mistake and a *brilliant* idea, time will tell.

Quick Example
-------------

Say you have a structure like this:

``` json
{
    "redundantField": 7,
    "notValid": false,
    "fingers_lh": 5,
    "fingers_rh": 5,
    "person": {
        "name": "ROEL",
        "species": "Rollulus rouloul"
    }
}
```

But you'd rather have had this:

``` json
{
    "valid": true,
    "name": "roel",
    "fingers": 10
}
```

Then give `Plumber` the schema of the desired structure along with:

``` lua
return pb.mapValues(function(u)
    return {
        valid = not u.notValid,
        name = u.person.name:lower(),
        fingers = u.fingers_lh + u.fingers_rh
    }
end)
```

And plumb:

    plumber.sh -s demo.avsc -l demo.lua -p demo.properties -i plumber-undesired -o plumber-desired

Optionally, you can give Plumber a bunch of inputs and a bunch of expected outputs. Prior to start the streaming job, it checks that given these inputs the provided logic yields these outputs. If not, it will refuse to start. An example test is found [here](examples/demo.test.lua).

Usage
-----

    plumber 0.0.2
    Usage: plumber [options]

      --help
            prints this usage text.
      -i <topic> | --source <topic>
            source topic.
      -o <topic> | --sink <topic>
            sink topic.
      -d <types> | --deserialize <types>
            how to deserialize input messages.
      -s <types> | --serialize <types>
            how to serialize output messages.
      -l <file> | --script <file>
            lua script to provide operations, e.g. demo.lua.
      -p <file> | --properties <file>
            properties file, e.g. demo.properties.
      -t <file> | --test <file>
            lua script file for test/verification pre-pass, e.g. demo.test.lua.
      -D | --dry-run
            dry-run, do no start streaming. Only makes sense in combination with -t.

    <types> has the format "keytype:valuetype" or simply "valuetype", where
    the type can be long, string, avro or void. In case of type avro, one can
    optionally give a schema file: avro=file.avsc.

    Example:

    plumber -l toy.lua -i source -o sink -p my.properties -d string,avro -s string,avro=out.avsc


Rationale
---------

I was fed up with copy/pasteing the same standard boiler plate code for a Kafka streams processor, and deploy tons of jars. I knew that I wanted to provide the transformation as a "configuration" for some fixed processing program. First, I considered to support [jq](https://stedolan.github.io/jq/), there is even a [Java lib](https://github.com/eiiches/jackson-jq) available, but decided that it was nice, but not flexible enough. Next, I wondered if there was something like XPath for JSON (yes there is, guess what: JSONPath), but rejected the idea for the same reasons as jq. After that, I considered good old friend `awk`, but it appears to be a bit out of fashion and to be honest: I don't even speak it myself. Finally, I recalled this funny language called Lua, and decided to simply give it a try, to see how it works out.
