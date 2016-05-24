#!/bin/bash
cd "$(dirname "${BASH_SOURCE}")"
java -jar target/kafka-streams-plumber-0.1-jar-with-dependencies.jar $@
