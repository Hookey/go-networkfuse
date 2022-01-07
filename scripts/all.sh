#!/bin/sh
set -eux

# Everything must compile on Linux
go build ./...

# Run the tests. Why the flags:
# -timeout 5m ... Get a backtrace on a hung test before the CI system kills us
# -p 1 .......... Run tests serially, which also means we get live output
#                 instead of per-package buffering.
# -count 1 ...... Disable result caching, so we can see flakey tests
go test -v -timeout 5m -p 1 -count 1 ./...

#make -C benchmark
#go test ./benchmark -test.bench '.*' -test.cpu 1,2
