#!/bin/bash

LOGFILE=$(date -u +%Y-%m-%dT%H:%M:%S).log

logLevel=INFO startLegacy=true legacy_verifyOnly=false nohup go run . -all=0 -blockchain=1 -legacy=1 -subtreevalidation=1 -blockvalidation=1 > $LOGFILE & 2>&1

rm -f current

ln -s $LOGFILE current
