#!/bin/bash

go build -o ubsv

# This command will start the legacy server in debug mode
logLevel=INFO startLegacy=true blockassembly_disabled=true legacy_verifyOnly=false dlv --listen=:4040 --accept-multiclient --headless=true --api-version=2 exec ./ubsv -- -all=0 -blockchain=1 -legacy=1 -subtreevalidation=1 -blockvalidation=1 -validator=1 
