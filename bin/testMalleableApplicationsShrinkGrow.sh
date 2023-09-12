#!/bin/bash

# Test script used to launch a test application and check that
# the application reacts correctly to the malleable instructions

CWD="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd $CWD/..

mvn clean install -DskipTests

HOSTNAME=`hostname`
HOSTFILE="hostfile"
echo $HOSTNAME > $HOSTFILE
echo $HOSTNAME >> $HOSTFILE
echo $HOSTNAME >> $HOSTFILE
echo $HOSTNAME >> $HOSTFILE

echo "Contents of the hostfile is:"
echo "<<<<"
cat $HOSTFILE
echo ">>>>"

MAINPRGM= java -cp "target/*" \
     --add-modules java.se \
     --add-exports java.base/jdk.internal.ref=ALL-UNNAMED \
     --add-opens java.base/java.lang=ALL-UNNAMED \
     --add-opens java.base/java.nio=ALL-UNNAMED  \
     --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
     --add-opens java.management/sun.management=ALL-UNNAMED \
     --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED \
     -Dapgas.verbose.launcher=false \
     -Dapgas.places=4 \
     -Dapgas.elastic=malleable \
     -Dapgas.hostfile=$HOSTFILE \
     -Dmalleable_scheduler_ip=127.0.0.1 \
     -Dmalleable_scheduler_port=8081 \
     apgas.impl.elastic.DummyApplication 30 &

sleep 5
java -cp "target/*" apgas.impl.elastic.MalleableOrder shrink 2
sleep 5
java -cp "target/*" apgas.impl.elastic.MalleableOrder grow 1 $HOSTNAME


wait $MAINPRGM

rm $HOSTFILE
