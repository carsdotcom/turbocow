#!/bin/bash

THISDIR=$(dirname $(readlink -e ${BASH_SOURCE[0]}))

set -x
cd $THISDIR
DEST=/code/backup/focus/ingestion-framework

[[ -d "$DEST" ]] || exit 1
mv $DEST/*Spec.scala .
