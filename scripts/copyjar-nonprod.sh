#!/bin/bash

ARGS_ARRAY=("${@}")
NUM_ARGS=${#ARGS_ARRAY[@]}
THISDIR=$(dirname $(readlink -e ${BASH_SOURCE[0]}))

set -x
cd $THISDIR/..

scp -p $(find ./target/scala-2.10/ingestionframework*.jar) msesterh@cj4hdl001.cars.com:/tmp/spark_jar/