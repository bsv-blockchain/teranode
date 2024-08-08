#!/bin/bash

LOGFILE=$(date -u +%Y-%m-%dT%H:%M:%S).log

logLevel=INFO startLegacy=true blockassembly_disabled=true nohup go run . -all=0 -blockchain=1 -legacy=1 -subtreevalidation=1 -blockvalidation=1 -validator=1 -blockpersister=1 -utxopersister=1 > $LOGFILE & 2>&1

rm -f current

ln -s $LOGFILE current
