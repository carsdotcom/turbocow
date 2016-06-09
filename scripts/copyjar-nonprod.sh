#!/bin/bash

ARGS_ARRAY=("${@}")
NUM_ARGS=${#ARGS_ARRAY[@]}
THISDIR=$(dirname $(readlink -e ${BASH_SOURCE[0]}))

set -x
cd $THISDIR/..

# Add a tag to identify the jar.
TAG="$1"
[ -n "$TAG" ] || TAG="$USERTAG"
[ -n "$TAG" ] || exit 1
echo "Using TAG=($TAG)"

SRC=$(find ./target/scala-2.10/ingestionframework*.jar)
[ -n "$SRC" ] || exit 2
[ -f "$SRC" ] || exit 3
BASE=$(basename $SRC)
DEST="${BASE%%.jar}-$TAG.jar"
echo "SRC=($SRC)"
echo "DEST=($DEST)"

scp -p $(find ./target/scala-2.10/ingestionframework*.jar) msesterh@cj4hdl001.cars.com:/tmp/spark_jar/$DEST
