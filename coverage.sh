SETTINGS_CONTEXT=test go test -tags "testtxmetacache,test_all,test_stores,test_model,test_services,test_util,test_smoke_rpc" -count=1 -coverprofile=1.out ./...

# go test -v -count 1 -tags test_smoke_rpc -coverprofile=3.out ./...

# cd test/services
# go test -v -count 1 -tags test_services -coverprofile=../../4.out ./... 
# cd ../..

# cd test/model
# go test -v -count 1 -tags test_model -coverprofile=../../5.out ./...
# cd ../..

# cd test/stores && \
# go test -v -count 1 -tags test_stores,testtxmetacache -coverprofile=../../6.out ./...
# cd ../..

# cd test/util && \
# go test -v -count 1 -tags test_util -coverprofile=../../7.out ./...
# cd ../..

# go install github.com/wadey/gocovmerge@latest
# gocovmerge 1.out 4.out 5.out 6.out 7.out > total.out

grep -v '.pb.go' 1.out > total2.out

go tool cover -func=total2.out
go tool cover -html=total2.out

# rm 1.out
