#!/bin/bash
CWD="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$CWD"/../ || exit
pwd
# Test script used to launch a test application and check that
# the application reacts correctly to the malleable instructions
mvn clean install -DskipTests

HOSTNAME=$(hostname)
HOSTFILE="hostfile"
echo "$HOSTNAME" >$HOSTFILE
echo "$HOSTNAME" >>$HOSTFILE
echo "$HOSTNAME" >>$HOSTFILE

echo "Contents of the hostfile is:"
echo "<<<<"
cat $HOSTFILE
echo ">>>>"

MAINPRGM= java -cp "target/*" \
  --add-modules java.se \
  --add-exports java.base/jdk.internal.ref=ALL-UNNAMED \
  --add-opens java.base/java.lang=ALL-UNNAMED \
  --add-opens java.base/java.nio=ALL-UNNAMED \
  --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens java.management/sun.management=ALL-UNNAMED \
  --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED \
  -Dapgas.verbose.launcher=true \
  -Dapgas.places=3 \
  -Dapgas.elastic=malleable \
  -Dapgas.hostfile=$HOSTFILE \
  -Delastic_scheduler_ip=127.0.0.1 \
  -Delastic_scheduler_port=8081 \
  apgas.impl.elastic.MalleableDummyApplication 25 &

sleep 5
java -cp "target/*" apgas.impl.elastic.ElasticOrder grow 2 $HOSTNAME $HOSTNAME
sleep 5
java -cp "target/*" apgas.impl.elastic.ElasticOrder shrink 1
sleep 5
java -cp "target/*" apgas.impl.elastic.ElasticOrder grow 2 $HOSTNAME $HOSTNAME

wait $MAINPRGM

rm $HOSTFILE
