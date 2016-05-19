#!/bin/bash

THISDIR=$(dirname $(readlink -e ${BASH_SOURCE[0]}))

set -x
cd $THISDIR
[[ -n "$1" && -f "$1" ]] || exit 1
FOCUSFILE="$1"

DEST=/code/backup/focus/ingestion-framework
mkdir -p $DEST

# move all over
mv *Spec.scala $DEST/

# move back unitspec and the focus file
mv -vf $DEST/UnitSpec.scala .
mv -vf $DEST/$FOCUSFILE .

