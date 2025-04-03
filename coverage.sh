TAGS="testtxmetacache,test_all,test_stores,test_model,test_services,test_util,test_smoke_rpc"

SETTINGS_CONTEXT=test \
  go test -race -count=1 -coverprofile=coverage.out -tags "${TAGS}" \
  $(go list -tags "${TAGS}" ./... | grep -v 'github.com/bitcoin-sv/teranode/cmd')


# go install github.com/wadey/gocovmerge@latest
# gocovmerge 1.out 4.out 5.out 6.out 7.out > total.out

grep -v --no-filename -e "\.pb\.go" \
        -e "github\.com/bitcoin-sv/teranode/cmd/" \
        -e "github\.com/bitcoin-sv/teranode/test/" \
        -e "github\.com/bitcoin-sv/teranode/tracing/" coverage.out > filtered.out


go tool cover -func=filtered.out | tail -1
go tool cover -html=filtered.out
