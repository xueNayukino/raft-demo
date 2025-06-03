# 分布式键值存储系统测试方案

本文档描述了基于etcd/raft的分布式键值存储系统的测试方案，重点关注Leader失效和重新选举场景。

## 测试架构

测试分为三个层次：

1. **单元测试**：测试各个组件的独立功能
2. **集成测试**：测试组件间的交互和系统整体功能
3. **故障注入测试**：模拟各种故障场景，测试系统的容错能力

## 单元测试

### 节点层测试 (`node/raft_node_test.go`)

- `TestSingleNodeLeaderElection`: 测试单节点集群的Leader选举
- `TestMultiNodeLeaderElection`: 测试多节点集群的Leader选举
- `TestLeaderFailover`: 测试Leader节点故障后的重新选举
- `TestLogReplication`: 测试日志复制功能
- `TestLogConsistency`: 测试日志一致性
- `TestVotingMechanism`: 测试投票机制
- `TestHeartbeatMechanism`: 测试心跳机制
- `TestStateMachineConsistency`: 测试状态机一致性

### 服务器层测试 (`server/server_test.go`)

- `TestServerLeaderElection`: 测试服务器层面的Leader选举和故障恢复
- `TestServerLeaderRedirection`: 测试非Leader节点的请求重定向
- `TestServerJoinCluster`: 测试节点加入集群

### 增强的Leader失效测试 (`node/enhanced_leader_failover_test.go`)

- `TestEnhancedLeaderFailover`: 测试多种Leader失效场景
- `TestLeaderReelection`: 测试Leader重新选举的稳定性
- `TestSplitBrainPrevention`: 测试脑裂预防机制

## 集成测试

### 基本集群测试 (`test_cluster.sh`)

- 测试集群启动
- 测试基本的键值操作
- 测试数据复制

### Leader故障测试 (`test_leader.sh`)

- 测试Leader节点宕机
- 测试新Leader选举
- 测试数据一致性

### 增强版Leader故障测试 (`test_leader_failover_enhanced.sh`)

- 测试单Leader节点故障
- 测试多节点同时故障
- 测试节点重启
- 测试网络分区（脑裂预防）

## 执行测试

### 单元测试

```bash
# 运行所有单元测试
go test ./...

# 运行特定测试
go test -v ./node -run TestLeaderFailover
go test -v ./server -run TestServerLeaderElection
go test -v ./node -run TestEnhancedLeaderFailover
```

### 集成测试

```bash
# 运行基本集群测试
./test_cluster.sh

# 运行Leader故障测试
./test_leader.sh

# 运行增强版Leader故障测试
./test_leader_failover_enhanced.sh
```

## 测试覆盖场景

1. **正常操作**
   - Leader选举
   - 日志复制
   - 键值读写
   - 节点加入

2. **故障场景**
   - 单节点故障
   - 多节点同时故障
   - Leader节点故障
   - 网络分区
   - 节点重启

3. **边缘情况**
   - 选举超时
   - 网络延迟
   - 脑裂预防
   - 状态不一致修复

## 测试结果分析

测试结果会保存在以下文件中：
- 单元测试：标准输出
- 集成测试：`test_result.log`

分析测试结果时，应关注：
1. 是否成功选出新Leader
2. Leader选举耗时
3. 数据一致性是否保持
4. 系统在故障后是否恢复正常功能

## 常见问题排查

1. **Leader选举失败**
   - 检查选举超时设置
   - 检查网络连接
   - 查看节点日志中的投票情况

2. **数据不一致**
   - 检查日志复制是否成功
   - 检查应用日志的顺序
   - 检查状态机实现

3. **节点无法加入集群**
   - 检查配置变更实现
   - 检查网络连接
   - 检查节点ID是否唯一

4. **脑裂问题**
   - 检查多数派确认机制
   - 检查网络分区检测
   - 检查Leader降级机制 