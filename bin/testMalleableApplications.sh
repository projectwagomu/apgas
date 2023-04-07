#!/bin/bash

# Test script used to launch a test application and check that the application reacts correctly to the malleable instructions

java -cp "../target/*" \
     -Dapgas.verbose.launcher=true \
     -Dapgas.places=4 \
     apgas.DummyApplication 10
