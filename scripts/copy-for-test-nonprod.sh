#!/bin/bash

ARGS_ARRAY=("${@}")
NUM_ARGS=${#ARGS_ARRAY[@]}
THISDIR=$(dirname $(readlink -e ${BASH_SOURCE[0]}))

set -x
cd $THISDIR/..

# If tag not specified, pull from env var "USERTAG"; pass to copyjar script.
TAG="$1"
[ -n "$TAG" ] || TAG="$USERTAG"
echo "Using TAG=($TAG)"

$THISDIR/copyjar-nonprod.sh $TAG || echo "FFFFFFFFFFFFFFFFFFFFFFFF FAIL"

scp -p ./notes/oncluster-avro-schema-test.json \
       ./notes/configuration-for-test-on-cluster.json  \
       msesterh@cj4hdl001.cars.com:/tmp/spark_jar/ || echo "FFFFFFFFFFFFFFFFFFFFFFFF FAIL"
#scp -p ./notes/oncluster-avro-schema-test.json  msesterh@cj4hdl001.cars.com:/tmp/spark_jar/ || echo "FFFFFFFFFFFFFFFFFFFFFFFF FAIL"
