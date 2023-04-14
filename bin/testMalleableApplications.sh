#!/bin/bash

# Test script used to launch a test application and check that the application reacts correctly to the malleable instructions
# mvn clean package

cd "$(dirname "$0")"

java -cp "../target/*" \
     -Dapgas.verbose.launcher=true \
     -Dapgas.places=4 \
     -Dapgas.elastic=malleable \
     -Dmalleable_scheduler_ip=127.0.0.1 \
     -Dmalleable_scheduler_port=8081 \
     apgas.testing.DummyApplication 10
