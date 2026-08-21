# Real-Time & Batch Lakehouse Sprint Lab (流批一体湖仓实战靶场)

本项目是一个面向生产级分布式架构与底层调优的轻量化流批一体实验靶场。项目采用**开发机（发压与计算作业提交）**与**数据机（微型分布式存储与消息集群）**分离架构，用于复现与攻坚高并发数据倾斜、Watermark 乱序入湖、Shuffle Spill 调优、小文件治理及执行计划分析。

---

## 🏗️ 架构拓扑与目录结构

```text
lakehouse-sprint/
├── data-node/                  # 【数据机】基础设施编排
│   ├── docker-compose.yml      # Kafka (KRaft) + MinIO + Iceberg REST Catalog
│   └── scripts/
│       └── init-topic.sh       # Kafka Topic 自动初始化脚本
├── dev-node/                   # 【开发机】流批计算与发压工程
│   └── producer/               # Go 高并发/倾斜造数引擎
│       ├── go.mod
│       ├── go.sum
│       └── main.go
└── README.md
```

---

## ⚡ 核心特性

1. **零 ZooKeeper 依赖**：数据节点采用 Kafka KRaft 模式，配合 MinIO (S3 API) 与 Apache Iceberg REST Catalog，资源占用低且原生适配 Apple Silicon。
2. **高性能多协程造数引擎**：基于 Go `kafka-go` 批量异步写入，轻松支撑单机 **50,000 ~ 100,000 QPS** 吞吐发压。
3. **精准故障注入**：
   * **85% 数据倾斜（Data Skew）**：将 85% 的订单流量固定打入特定超级 Key（如 `88888888` 等），用于下游 Spark/Flink 倾斜与 Task 长尾调优。
   * **事件乱序（Out-of-Order Events）**：10% 概率注入 1~10 秒的事件时间滞后，用于验证 Flink Watermark 容忍度。

---

## 🚀 快速启动指南

### 步骤 1：启动数据机基础设施 (Data Node)

1. 获取数据机局域网主机名或 IP：
   ```bash
   echo "$(hostname -s).local"
   # 或查看 IP: ipconfig getifaddr en0
   ```
2. 确认 `data-node/docker-compose.yml` 中 `KAFKA_ADVERTISED_LISTENERS` 的配置为主机名（例如 `datamac.local:9092`）。
3. 启动集群并初始化 Topic：
   ```bash
   cd data-node
   docker compose up -d
   chmod +x scripts/init-topic.sh
   ./scripts/init-topic.sh
   ```

* **MinIO Console**: `http://<数据机IP>:9001` (账号: `admin` / 密码: `password123`)
* **Iceberg REST Endpoint**: `http://<数据机IP>:8181`

---

### 步骤 2：启动开发机造数引擎 (Dev Node)

在开发机中进入 `producer` 目录并运行：

```bash
cd dev-node/producer

# 安装依赖
go mod download

# 1. 常规 5w QPS 发压（注入 85% 倾斜）
go run main.go -broker="datamac.local:9092" -qps=50000 -skew=true -workers=8

# 2. 极限 10w QPS 梯度发压
go run main.go -broker="datamac.local:9092" -qps=100000 -skew=true -workers=16
```

---

## 🔍 数据与连通性验证

在**数据机**上验证 Kafka 消息实时写入：

```bash
docker exec -it lab-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic user_orders \
  --from-beginning \
  --max-messages 5
```

---

## 📅 演练推进路线

* [x] **Day 1 - 2**: 跨机微型集群搭建与 Go 高并发/倾斜造数引擎
* [ ] **Day 3 - 4**: Flink 实时流式入湖 (Apache Iceberg) + Watermark 乱序处理
* [ ] **Day 5**: Spark 离线分层建模 (ODS $\to$ DWD $\to$ DWS)
* [ ] **Day 6 - 7**: DuckDB 千万级大表 SQL 执行计划 (EXPLAIN ANALYZE) 深度攻坚
* [ ] **Day 8 - 14**: 生产级故障复现（数据倾斜、Spill to Disk、Flink 反压、小文件治理）
* [ ] **Day 15 - 21**: 架构设计反问、故障排查话术与简历 STAR 复盘
