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
  -Dapgas.threads=8 \
  -Dapgas.immediate.threads=8 \
  -Dapgas.elastic=evolving \
  -Dapgas.lowload=10 \
  -Dapgas.highload=90 \
  -Dapgas.hyperthreading=false \
  -Dapgas.hostfile=$HOSTFILE \
  -Delastic_scheduler_ip=127.0.0.1 \
  -Delastic_scheduler_port=8081 \
  apgas.impl.elastic.EvolvingDummyApplication 50 &

#this scenario will shrink from 3 to 1 place because there is no CPU load of EvolvingDummyApplication

wait $MAINPRGM

rm $HOSTFILE
