#!/bin/bash
base_path=$( cd "$(dirname "${BASH_SOURCE}")" ; pwd -P )
java -jar $base_path/target/kafka-streams-plumber-0.1-jar-with-dependencies.jar $@
