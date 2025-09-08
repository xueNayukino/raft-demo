# Makefile for qklzl project

# 变量定义
APP_NAME := qklzl
CMD_DIR := ./cmd/qklzl
BUILD_DIR := ./bin
PROTO_DIR := ./internal/transport

# Go 相关变量
GO := go
GOFLAGS := -ldflags="-s -w"

# 默认目标
.PHONY: all
all: clean build

# 构建应用程序
.PHONY: build
build:
	@echo "构建 $(APP_NAME)..."
	@mkdir -p $(BUILD_DIR)
	$(GO) build $(GOFLAGS) -o $(BUILD_DIR)/$(APP_NAME) $(CMD_DIR)
	@echo "构建完成: $(BUILD_DIR)/$(APP_NAME)"

# 清理构建文件
.PHONY: clean
clean:
	@echo "清理构建文件..."
	@rm -rf $(BUILD_DIR)
	@echo "清理完成"

# 运行测试
.PHONY: test
test:
	@echo "运行测试..."
	$(GO) test -v ./...

# 运行测试并生成覆盖率报告
.PHONY: test-coverage
test-coverage:
	@echo "运行测试并生成覆盖率报告..."
	$(GO) test -v -coverprofile=coverage.out ./...
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "覆盖率报告生成: coverage.html"

# 格式化代码
.PHONY: fmt
fmt:
	@echo "格式化代码..."
	$(GO) fmt ./...

# 代码检查
.PHONY: vet
vet:
	@echo "代码检查..."
	$(GO) vet ./...

# 生成 protobuf 文件
.PHONY: proto
proto:
	@echo "生成 protobuf 文件..."
	cd $(PROTO_DIR) && protoc --go_out=. --go-grpc_out=. *.proto
	@echo "protobuf 文件生成完成"

# 安装依赖
.PHONY: deps
deps:
	@echo "安装依赖..."
	$(GO) mod download
	$(GO) mod tidy

# 运行应用程序（开发模式）
.PHONY: run
run: build
	@echo "运行 $(APP_NAME)..."
	$(BUILD_DIR)/$(APP_NAME) -id 1 -bootstrap -raft-addr 127.0.0.1:12001 -http-addr 127.0.0.1:8001

# 运行多节点集群（用于测试）
.PHONY: run-cluster
run-cluster: build
	@echo "启动3节点集群..."
	@mkdir -p data1 data2 data3
	@echo "启动节点1（引导节点）..."
	$(BUILD_DIR)/$(APP_NAME) -id 1 -bootstrap -raft-addr 127.0.0.1:12001 -http-addr 127.0.0.1:8001 -data-dir ./data1 &
	@sleep 2
	@echo "启动节点2..."
	$(BUILD_DIR)/$(APP_NAME) -id 2 -join 127.0.0.1:8001 -raft-addr 127.0.0.1:12002 -http-addr 127.0.0.1:8002 -data-dir ./data2 &
	@sleep 2
	@echo "启动节点3..."
	$(BUILD_DIR)/$(APP_NAME) -id 3 -join 127.0.0.1:8001 -raft-addr 127.0.0.1:12003 -http-addr 127.0.0.1:8003 -data-dir ./data3 &
	@echo "集群启动完成"

# 停止所有节点
.PHONY: stop
stop:
	@echo "停止所有节点..."
	@pkill -f "$(APP_NAME)" || true
	@echo "所有节点已停止"

# 帮助信息
.PHONY: help
help:
	@echo "可用的命令:"
	@echo "  build        - 构建应用程序"
	@echo "  clean        - 清理构建文件"
	@echo "  test         - 运行测试"
	@echo "  test-coverage- 运行测试并生成覆盖率报告"
	@echo "  fmt          - 格式化代码"
	@echo "  vet          - 代码检查"
	@echo "  proto        - 生成 protobuf 文件"
	@echo "  deps         - 安装依赖"
	@echo "  run          - 运行应用程序（单节点）"
	@echo "  run-cluster  - 运行3节点集群"
	@echo "  stop         - 停止所有节点"
	@echo "  help         - 显示帮助信息"