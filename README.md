# qklzl - 基于Raft共识算法的分布式键值存储系统

qklzl是一个基于Raft共识算法的分布式键值存储系统，使用Go语言开发，底层存储基于BadgerDB。本系统实现了完整的Raft协议，支持领导者选举、日志复制、成员变更、快照等功能。

## 快速开始

### 环境准备

确保已安装Go 1.19或更高版本。

### 编译

```bash
go build -o bin/qklzl cmd/qklzl/main.go
```

### 启动集群

1. 创建数据目录：

```bash
mkdir -p data1 data2 data3
```

2. 启动引导节点（节点1）：

```bash
./bin/qklzl -id 1 -data-dir ./data1 -bootstrap -raft-addr 127.0.0.1:12001 -http-addr 127.0.0.1:8001
```

3. 启动节点2并加入集群：

```bash
./bin/qklzl -id 2 -data-dir ./data2 -join 127.0.0.1:8001 -raft-addr 127.0.0.1:12002 -http-addr 127.0.0.1:8002
```

4. 启动节点3并加入集群：

```bash
./bin/qklzl -id 3 -data-dir ./data3 -join 127.0.0.1:8001 -raft-addr 127.0.0.1:12003 -http-addr 127.0.0.1:8003
```

### 验证集群状态

启动完成后，可以通过以下命令检查集群状态：

```bash
# 检查所有节点状态
curl http://127.0.0.1:8001/status
curl http://127.0.0.1:8002/status
curl http://127.0.0.1:8003/status
```

正常情况下，应该看到一个节点成为领导者（is_leader: true），其他节点为跟随者。

### 基本功能测试

#### 1. 测试写入操作

```bash
# 写入键值对
curl -X POST -H "Content-Type: application/json" -d '{"op":"put","key":"test1","value":"hello world"}' http://127.0.0.1:8001/kv
```

#### 2. 测试读取操作

```bash
# 从不同节点读取数据，验证一致性
curl "http://127.0.0.1:8001/kv?key=test1"
curl "http://127.0.0.1:8002/kv?key=test1"
curl "http://127.0.0.1:8003/kv?key=test1"
```

#### 3. 测试删除操作

```bash
# 删除键值对
curl -X POST -H "Content-Type: application/json" -d '{"op":"delete","key":"test1"}' http://127.0.0.1:8001/kv

# 验证删除结果
curl "http://127.0.0.1:8002/kv?key=test1"
```

#### 4. 测试故障转移

```bash
# 停止当前领导者节点（假设是节点1）
# 使用 Ctrl+C 或 kill 命令停止进程

# 等待几秒钟后检查剩余节点状态
curl http://127.0.0.1:8002/status
curl http://127.0.0.1:8003/status

# 通过新的领导者写入数据
curl -X POST -H "Content-Type: application/json" -d '{"op":"put","key":"test_failover","value":"leader election works"}' http://127.0.0.1:8002/kv
```

## 详细测试用例

以下是一系列详细的测试用例，用于验证系统是否符合Raft算法的各项特性。

### 1. 测试领导者选举

检查各节点状态，验证领导者选举是否成功：

```bash
# 查看所有节点状态
echo "节点1状态：" && curl http://127.0.0.1:8001/status
echo "节点2状态：" && curl http://127.0.0.1:8002/status
echo "节点3状态：" && curl http://127.0.0.1:8003/status
```

预期结果：应该看到一个节点（通常是节点1）成为领导者（is_leader: true），所有节点报告相同的leader_id和term。

### 2. 测试日志复制与一致性

写入数据并验证所有节点是否同步：

```bash
# 写入数据
curl -X POST -H "Content-Type: application/json" -d '{"op":"put","key":"key1","value":"value1"}' http://127.0.0.1:8001/kv
curl -X POST -H "Content-Type: application/json" -d '{"op":"put","key":"key2","value":"value2"}' http://127.0.0.1:8001/kv

# 从所有节点读取数据验证一致性
echo "节点1读取key1：" && curl "http://127.0.0.1:8001/kv?key=key1"
echo "节点2读取key1：" && curl "http://127.0.0.1:8002/kv?key=key1"
echo "节点3读取key1：" && curl "http://127.0.0.1:8003/kv?key=key1"

echo "节点1读取key2：" && curl "http://127.0.0.1:8001/kv?key=key2"
echo "节点2读取key2：" && curl "http://127.0.0.1:8002/kv?key=key2"
echo "节点3读取key2：" && curl "http://127.0.0.1:8003/kv?key=key2"
```

预期结果：所有节点都能返回相同的值，证明日志已成功复制到所有节点。

### 3. 测试安全性（Safety）

#### 3.1 测试非领导者节点不能写入数据

```bash
# 尝试通过非领导者节点（节点2）写入数据
curl -X POST -H "Content-Type: application/json" -d '{"op":"put","key":"key3","value":"value3"}' http://127.0.0.1:8002/kv
```

预期结果：返回错误消息，表明非Leader节点不能执行写操作，并提供Leader节点信息。

### 4. 测试领导者故障和自动选举

模拟领导者节点故障，验证集群能否自动选举新领导者：

```bash
# 找出当前领导者节点的进程ID
ps aux | grep "qklzl -id 1" | grep -v grep

# 终止领导者节点
kill <进程ID>

# 等待几秒钟，让集群选出新的领导者
sleep 10

# 检查剩余节点的状态
echo "节点2状态：" && curl http://127.0.0.1:8002/status
echo "节点3状态：" && curl http://127.0.0.1:8003/status
```

预期结果：其中一个剩余节点应该成为新的领导者，term值应该增加。

### 5. 测试新领导者能否处理写请求

```bash
# 通过新的领导者写入数据
curl -X POST -H "Content-Type: application/json" -d '{"op":"put","key":"key3","value":"value3"}' http://127.0.0.1:8002/kv

# 验证数据是否成功写入
curl "http://127.0.0.1:8002/kv?key=key3"
curl "http://127.0.0.1:8003/kv?key=key3"
```

预期结果：新领导者能够成功处理写请求，数据在剩余节点间保持一致。

### 6. 测试节点恢复

重启之前故障的节点，验证它能否成功重新加入集群并同步数据：

```bash
# 重启节点1
./qklzl -id 1 -data-dir ./data1 -join 127.0.0.1:8002 -raft-addr 127.0.0.1:12001 -http-addr 127.0.0.1:8001

# 等待节点1完全加入集群
sleep 10

# 检查节点1的状态
curl http://127.0.0.1:8001/status

# 验证节点1是否能读取到它故障期间写入的数据
curl "http://127.0.0.1:8001/kv?key=key3"
```

预期结果：节点1成功重新加入集群，能够读取到它故障期间写入的数据。

### 7. 测试网络分区

模拟网络分区（将节点3与集群隔离）：

```bash
# 终止节点3
ps aux | grep "qklzl -id 3" | grep -v grep
kill <进程ID>

# 验证剩余节点仍然可以正常工作
curl -X POST -H "Content-Type: application/json" -d '{"op":"put","key":"key4","value":"value4"}' http://127.0.0.1:8002/kv
curl "http://127.0.0.1:8001/kv?key=key4"
curl "http://127.0.0.1:8002/kv?key=key4"
```

预期结果：剩余节点能够正常处理写请求，数据在剩余节点间保持一致。

### 8. 测试多数节点故障

终止节点2和节点3，模拟多数节点故障：

```bash
# 终止节点2
ps aux | grep "qklzl -id 2" | grep -v grep
kill <进程ID>

# 尝试通过节点1写入数据（应该会失败，因为没有多数派）
curl -X POST -H "Content-Type: application/json" -d '{"op":"put","key":"key5","value":"value5"}' http://127.0.0.1:8001/kv
```

预期结果：写入请求应该失败，因为没有足够的节点形成多数派。

### 9. 测试数据持久性

关闭所有节点，然后重新启动，验证数据是否持久化：

```bash
# 关闭所有节点
pkill -f qklzl

./qklzl -id 1 -data-dir ./data1 -bootstrap -raft-addr 127.0.0.1:12001 -http-addr 127.0.0.1:8001
./qklzl -id 2 -data-dir ./data2 -join 127.0.0.1:8001 -raft-addr 127.0.0.1:12002 -http-addr 127.0.0.1:8002
./qklzl -id 3 -data-dir ./data3 -join 127.0.0.1:8001 -raft-addr 127.0.0.1:12003 -http-addr 127.0.0.1:8003

# 重新启动所有节点
./qklzl -id 1 -data-dir ./data1 -bootstrap -raft-addr 127.0.0.1:12001 -http-addr 127.0.0.1:8001
sleep 5
./qklzl -id 2 -data-dir ./data2 -join 127.0.0.1:8001 -raft-addr 127.0.0.1:12002 -http-addr 127.0.0.1:8002
sleep 5
./qklzl -id 3 -data-dir ./data3 -join 127.0.0.1:8001 -raft-addr 127.0.0.1:12003 -http-addr 127.0.0.1:8003
sleep 10

# 验证之前写入的数据是否仍然存在
echo "key1：" && curl "http://127.0.0.1:8001/kv?key=key1"
echo "key2：" && curl "http://127.0.0.1:8001/kv?key=key2"
echo "key3：" && curl "http://127.0.0.1:8001/kv?key=key3"
echo "key4：" && curl "http://127.0.0.1:8001/kv?key=key4"
```

预期结果：重启后，之前写入的所有数据仍然可以访问，证明数据持久性。

### 10. 测试指定Leader重启

测试在重启集群时指定特定节点作为Leader：

```bash
# 关闭所有节点
pkill -f qklzl

# 先启动节点2作为引导节点
./qklzl -id 2 -data-dir ./data2 -bootstrap -raft-addr 127.0.0.1:12002 -http-addr 127.0.0.1:8002
sleep 5

# 启动节点3和节点1加入集群
./qklzl -id 3 -data-dir ./data3 -join 127.0.0.1:8002 -raft-addr 127.0.0.1:12003 -http-addr 127.0.0.1:8003
sleep 5
./qklzl -id 1 -data-dir ./data1 -join 127.0.0.1:8002 -raft-addr 127.0.0.1:12001 -http-addr 127.0.0.1:8001
sleep 10

# 检查节点状态，验证节点2是否成为领导者
echo "节点1状态：" && curl http://127.0.0.1:8001/status
echo "节点2状态：" && curl http://127.0.0.1:8002/status
echo "节点3状态：" && curl http://127.0.0.1:8003/status
```

预期结果：节点2应该成为领导者，所有节点报告节点2为leader_id。

## API接口

### 状态查询
```
GET /status
```

### 键值操作
```
# 读取
GET /kv?key=<key>

# 写入
POST /kv
Content-Type: application/json
{"op":"put","key":"<key>","value":"<value>"}

# 删除
POST /kv
Content-Type: application/json
{"op":"delete","key":"<key>"}
```

## 清理资源

测试完成后，可以使用以下命令清理环境：

```bash
# 停止所有节点
pkill -f qklzl

# 删除数据目录
rm -rf data1 data2 data3

# 删除编译生成的可执行文件
rm -f bin/qklzl
```

## 项目架构

本项目采用分层架构设计：

- **应用层 (cmd/qklzl)**: 程序入口和命令行参数处理
- **服务层 (internal/service)**: 业务逻辑处理
- **仓库层 (internal/repository)**: 数据访问抽象层
- **存储层 (internal/storage)**: 底层数据存储实现
- **节点层 (internal/node)**: Raft协议实现
- **传输层 (internal/transport)**: 网络通信
- **状态机 (internal/fsm)**: 状态机实现

## 开发和测试

### 运行单元测试

```bash
# 运行所有测试
go test ./...

# 运行特定模块测试
go test ./internal/repository -v
go test ./internal/service -v
go test ./internal/storage -v
```

### 代码覆盖率

```bash
# 生成覆盖率报告
go test ./... -coverprofile=coverage.out
go tool cover -html=coverage.out -o coverage.html
```