mkdir -p data1 data2 data3

   # 启动引导节点（节点1）
   ./qklzl -id 1 -data-dir ./data1 -bootstrap -raft-addr 127.0.0.1:12001 -http-addr 127.0.0.1:8001
   
   # 启动节点2并加入集群
   ./qklzl -id 2 -data-dir ./data2 -join 127.0.0.1:8001 -raft-addr 127.0.0.1:12002 -http-addr 127.0.0.1:8002
   
   # 启动节点3并加入集群
   ./qklzl -id 3 -data-dir ./data3 -join 127.0.0.1:8001 -raft-addr 127.0.0.1:12003 -http-addr 127.0.0.1:8003

      curl http://127.0.0.1:8001/status
   curl http://127.0.0.1:8002/status
   curl http://127.0.0.1:8003/status

      # 写入数据
   curl -X POST -H "Content-Type: application/json" -d '{"op":"put","key":"test-key1","value":"test-value1"}' http://127.0.0.1:8001/kv
   
   # 读取数据
   curl "http://127.0.0.1:8001/kv?key=test-key1"
   curl "http://127.0.0.1:8002/kv?key=test-key1"
   curl "http://127.0.0.1:8003/kv?key=test-key1"
   
   # 删除数据
   curl -X POST -H "Content-Type: application/json" -d '{"op":"delete","key":"test-key1"}' http://127.0.0.1:8001/kv
   
   # 验证删除
   curl "http://127.0.0.1:8001/kv?key=test-key1"

      # 找出并终止Leader节点
   ps aux | grep "qklzl -id 1" | grep -v grep
   kill <进程ID>
   
   # 检查新Leader
   curl http://127.0.0.1:8002/status
   curl http://127.0.0.1:8003/status
   
   # 在新Leader上写入数据
   curl -X POST -H "Content-Type: application/json" -d '{"op":"put","key":"test-key2","value":"test-value2"}' http://127.0.0.1:8003/kv
   
   # 验证数据复制
   curl "http://127.0.0.1:8002/kv?key=test-key2"

      # 重启故障节点
   ./qklzl -id 1 -data-dir ./data1 -join 127.0.0.1:8003 -raft-addr 127.0.0.1:12001 -http-addr 127.0.0.1:8001
   
   # 检查节点状态
   curl http://127.0.0.1:8001/status
   
   # 验证数据同步
   curl "http://127.0.0.1:8001/kv?key=test-key2"

      # 写入持久化测试数据
   curl -X POST -H "Content-Type: application/json" -d '{"op":"put","key":"persistence-key","value":"test-value"}' http://127.0.0.1:8003/kv
   
   # 停止所有节点
   pkill -f qklzl
   
   # 重启集群
   ./qklzl -id 1 -data-dir ./data1 -bootstrap -raft-addr 127.0.0.1:12001 -http-addr 127.0.0.1:8001
   ./qklzl -id 2 -data-dir ./data2 -join 127.0.0.1:8001 -raft-addr 127.0.0.1:12002 -http-addr 127.0.0.1:8002
   ./qklzl -id 3 -data-dir ./data3 -join 127.0.0.1:8001 -raft-addr 127.0.0.1:12003 -http-addr 127.0.0.1:8003
   
   # 验证数据持久性
   curl "http://127.0.0.1:8001/kv?key=persistence-key"
   curl "http://127.0.0.1:8002/kv?key=persistence-key"
   curl "http://127.0.0.1:8003/kv?key=persistence-key"

      # 写入持久化测试数据
   curl -X POST -H "Content-Type: application/json" -d '{"op":"put","key":"persistence-key","value":"test-value"}' http://127.0.0.1:8003/kv
   
   # 停止所有节点
   pkill -f qklzl
   
   # 重启集群
   ./qklzl -id 1 -data-dir ./data1 -bootstrap -raft-addr 127.0.0.1:12001 -http-addr 127.0.0.1:8001
   ./qklzl -id 2 -data-dir ./data2 -join 127.0.0.1:8001 -raft-addr 127.0.0.1:12002 -http-addr 127.0.0.1:8002
   ./qklzl -id 3 -data-dir ./data3 -join 127.0.0.1:8001 -raft-addr 127.0.0.1:12003 -http-addr 127.0.0.1:8003
   
   # 验证数据持久性
   curl "http://127.0.0.1:8001/kv?key=persistence-key"
   curl "http://127.0.0.1:8002/kv?key=persistence-key"
   curl "http://127.0.0.1:8003/kv?key=persistence-key"