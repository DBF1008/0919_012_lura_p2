#!/usr/bin/env bash
#
# Manual unit test script for the circuit breaker feature.
#
# Covers:
#   - proxy/circuitbreaker.go  (three-state breaker, sd integration, concurrency)
#   - proxy/factory.go         (breaker wired into the proxy stack)
#   - config/config.go         (Backend.CircuitBreaker config field)
#   - sd/                      (service discovery layer used by the breaker)
#
# Usage:
#   ./test.sh            run all the unit tests below
#   ./test.sh -short     run only the circuit breaker unit tests

set -euo pipefail
cd "$(dirname "$0")"

echo "==> 1/5 build"
go build ./...

echo "==> 2/5 circuit breaker unit tests (race detector)"
go test -race -v -run 'CircuitBreaker' ./proxy/

if [[ "${1:-}" == "-short" ]]; then
	echo "==> short mode: done"
	exit 0
fi

echo "==> 3/5 config package unit tests"
go test -race -v ./config/

echo "==> 4/5 sd package unit tests"
go test -race -v ./sd/

echo "==> 5/5 proxy package unit tests"
go test -race -v ./proxy/

echo "==> all unit tests passed"
