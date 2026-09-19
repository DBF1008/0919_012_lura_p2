#!/usr/bin/env bash
# 熔断器功能迭代 - 单元测试脚本
# 用法: ./test.sh
set -euo pipefail
cd "$(dirname "$0")"

echo "==> 1. 编译与静态检查"
go build ./...
go vet ./config/ ./sd/... ./proxy/

echo "==> 2. config 包单元测试 (Backend 新增 CircuitBreaker 配置)"
go test -v ./config/

echo "==> 3. sd 包单元测试 (服务发现/负载均衡)"
go test -v ./sd/...

echo "==> 4. 熔断器单元测试 (含 -race 并发安全检测)"
go test -race -v -run 'TestCircuitBreaker' ./proxy/

echo "==> 5. proxy 包全部单元测试 (factory/balancing 集成)"
go test -v ./proxy/

echo "==> 6. 全仓库回归测试"
go test ./...

echo "ALL TESTS PASSED"
