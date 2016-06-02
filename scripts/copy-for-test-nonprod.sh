#!/bin/bash

ARGS_ARRAY=("${@}")
NUM_ARGS=${#ARGS_ARRAY[@]}
THISDIR=$(dirname $(readlink -e ${BASH_SOURCE[0]}))

set -x
cd $THISDIR/..

$THISDIR/copyjar-nonprod.sh || echo "FFFFFFFFFFFFFFFFFFFFFFFF FAIL"

scp -p ./notes/oncluster-avro-schema-test.json \
       ./notes/configuration-for-test-on-cluster.json  \
       msesterh@cj4hdl001.cars.com:/tmp/spark_jar/ || echo "FFFFFFFFFFFFFFFFFFFFFFFF FAIL"
#scp -p ./notes/oncluster-avro-schema-test.json  msesterh@cj4hdl001.cars.com:/tmp/spark_jar/ || echo "FFFFFFFFFFFFFFFFFFFFFFFF FAIL"
