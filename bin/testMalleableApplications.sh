#!/bin/bash

# Test script used to launch a test application and check that the application reacts correctly to the malleable instructions
mvn clean package -DskipTests

HSTNAME=`hostname`

cd "$(dirname "$0")"

MAINPRGM= java -cp "../target/*" \
     -Dapgas.verbose.launcher=true \
     -Dapgas.places=4 \
     -Dapgas.elastic=malleable \
     -Dmalleable_scheduler_ip=127.0.0.1 \
     -Dmalleable_scheduler_port=8081 \
     apgas.testing.DummyApplication 20 &

sleep 10

java -cp "../target/*" apgas.testing.MalleableOrder expand 1 $HSTNAME

sleep 4

java -cp "../target/*" apgas.testing.MalleableOrder shrink 2 $HSTNAME

wait $MAINPRGM
