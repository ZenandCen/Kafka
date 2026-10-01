# WalletKafkaSystem - Enterprise Wallet Architecture with Kafka, OpenSearch & Stream Processing

Hệ thống ví điện tử enterprise (.NET 8) với kiến trúc event-driven hoàn chỉnh, tích hợp đầy đủ hệ sinh thái:

- **Kafka 3 Brokers (KRaft Mode)**: `Acks=All`, `EnableIdempotence=true`, `LingerMs=10`, `Snappy Compression`
- **Kafka Streams / Stream Processing**: Windowing (1-minute Tumbling Window), Velocity Fraud Detection
- **OpenSearch & k-NN Vector Search**: Full-Text Search + Semantic Search (Cosine Similarity, 128-dim HNSW)
- **Kafka Connect**: OpenSearch Sink Connector (no-code ETL)
- **Redis In-Memory State Machine**: Lua Script Optimistic Locking (CAS), TTL Phase-Mismatch Resolution, Redis Stream Write-Behind DB Sync
- **PostgreSQL 16**: Source of Truth Database
- **Wikipedia Live EventStream Ingestion**: Wikimedia SSE -> Kafka

---

## Kienv truc he thong

```text
                            ┌────────────────────────────────────────┐
                            │  Wikimedia Stream / Wikipedia Ingestion│
                            └───────────────────┬────────────────────┘
                                                │
 [ Client / Producer API ]                      ▼
            │                         ┌──────────────────────────┐
            │ Message Key = userId    │ Kafka Topic:             │
            ▼                         │ `wikipedia-events`       │
 ┌──────────────────────────┐         └────────────┬─────────────┘
 │ Kafka Topic:             │                      │
 │ `wallet-transactions`    │                      │
 └──────────┬───────────────┘                      │
            │                                      │
            ├──────────────────────────────────────┼────────────────────────┐
            │                                      │                        │
            ▼                                      ▼                        ▼
 ┌──────────────────────────┐         ┌──────────────────────────┐ ┌──────────────────────────┐
 │  WalletStreamProcessor   │         │  WalletConsumer Worker   │ │  Kafka Connect Cluster   │
 │  (Real-time Windowing &  │         │  (Cooperative Sticky &   │ │  (OpenSearch Sink        │
 │  Velocity Fraud Alert)   │         │  Manual Offset Commit)   │ │  Connector)             │
 └──────────┬───────────────┘         └────────────┬─────────────┘ └────────────┬─────────────┘
            │                                      │                            │
            ▼ (Fraud Alert)                        ▼ (Index Audit & Vector)     ▼ (Sink Auto)
 ┌──────────────────────────┐         ┌────────────────────────────────────────────────────────┐
 │ Kafka Topic:             │         │       OPENSEARCH SEARCH ENGINE                        │
 │ `wallet-fraud-alerts`    │────────►│  (Full-text Search & k-NN Vector Semantic Search)     │
 └──────────────────────────┘         └────────────────────────────────────────────────────────┘
```

---

## Ca cau hinh

```
Kafka/
├── docker-compose.yml              # Full infrastructure (Kafka, Redis, OpenSearch, etc.)
├── connectors/
│   ├── opensearch-sink-wallet.json     # Kafka Connect: wallet topics -> OpenSearch
│   └── opensearch-sink-wikipedia.json  # Kafka Connect: wikipedia topic -> OpenSearch
├── WalletProducer/
│   ├── Models/
│   │   ├── TransactionEvent.cs
│   │   └── WikipediaChangeEvent.cs
│   ├── Services/
│   │   ├── WalletProducerService.cs
│   │   └── WikipediaStreamProducerService.cs
│   ├── Program.cs
│   └── WalletProducer.csproj
├── WalletConsumer/
│   ├── Models/
│   │   └── SemanticDocument.cs
│   ├── Services/
│   │   ├── WalletConsumerWorker.cs
│   │   ├── WalletRedisStateService.cs
│   │   ├── OpenSearchIndexingService.cs
│   │   ├── SemanticSearchService.cs
│   │   └── DbBatchSyncWorker.cs
│   ├── Program.cs
│   └── WalletConsumer.csproj
├── WalletStreamProcessor/
│   ├── Models/
│   │   └── WindowedAggregations.cs
│   ├── Services/
│   │   └── TransactionStreamProcessorWorker.cs
│   ├── Program.cs
│   └── WalletStreamProcessor.csproj
└── WalletKafkaSystem.slnx
```

---

## Huong dan khoi chao

### 1. Khoi dong ca hap infra Docker

```bash
docker compose up -d
```

Hạ tầng bao gồm:

| Service | Port | Mo ta |
|---------|------|-------|
| Kafka 1 | `9092` | Broker 1 (KRaft) |
| Kafka 2 | `9093` | Broker 2 (KRaft) |
| Kafka 3 | `9094` | Broker 3 (KRaft) |
| Kafka Connect | `8083` | Distributed Connect Cluster |
| OpenSearch | `9200` | Search Engine + k-NN Vectors |
| OpenSearch Dashboards | `5601` | UI for OpenSearch |
| Kafka-UI | `8080` | Kafka Dashboard |
| Redis | `6379` | State Machine + Cache |
| PostgreSQL | `5432` | Source of Truth (`wallet_db`) |

### 2. Kich hoat Kafka Connect OpenSearch Sink Connector

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/opensearch-sink-wallet.json

curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/opensearch-sink-wikipedia.json
```

### 3. Chay Kafka Stream Processor (Fraud Detection)

```bash
dotnet run --project WalletStreamProcessor
```

Worker theo doi luong giao dich `wallet-transactions`, tinh toan Tumbling Window 1 phut, phat hien gian lan (Velocity Fraud Detection) va ban canh bao sang topic `wallet-fraud-alerts`.

### 4. Chay Consumer Worker (Redis + OpenSearch + Semantic Indexing)

```bash
dotnet run --project WalletConsumer
```

Consumer doc song song `wallet-transactions` va `wikipedia-events`, thuc thi Redis Optimistic Locking, index document kem Vector Embedding vao OpenSearch, Manual Offset Commit.

### 5. Chay Producer CLI

```bash
dotnet run --project WalletProducer
```

Các che do lua chon:

| Che do | Mo ta |
|--------|-------|
| **1** | Nhap giao dich thu cong (`DEPOSIT` / `PAYMENT`) |
| **2** | Gia pho luong Lech Pha (PAYMENT truoc, DEPOSIT sau) - Auto Matching |
| **3** | Khoi dong Wikipedia Live EventStream (Wikimedia SSE -> Kafka) |
| **4** | Ban High Throughput 1,000 giao dich (Benchmark Snappy Compression) |

### 6. Thu nghiem Semantic Search (Vector Embedding)

```bash
dotnet run --project WalletConsumer -- --semantic-search "Thanh toan don hang bi thieu tien va nap tien"
```

Truy van k-NN Vector (Cosine Similarity) tren index `wallet-audit-semantic-index` cu OpenSearch.

---

## Nghiep vu co ban

### Phase Mismatch Resolution (Lech Pha)

Khi nguoi dung thuc hien PAYMENT khi chua co du tien:
1. Redis lua script kiem tra balance < required_amount
2. Luu pending payment voi `SETEX key 900 orderId` (TTL = 15 phut)
3. Khi DEPOSIT den tiep, lua script kiem tra pending key ton tai
4. Auto-match: Xoa pending key, ghi log matched_order

### Optimistic Locking (CAS)

Moi gia dich vao wallet thuc thi atomic Lua script:
```lua
HMGET balance, version, status  -- Doc hien tai
-- Validate (status != LOCKED)
HSET balance, version+1         -- Ghi lai voi version tang
```

### Velocity Fraud Detection

| Quy tac | Nguong | Hanh dong |
|---------|--------|-----------|
| Velocity | > 5 tx / 60s | Ban alert `wallet-fraud-alerts` |
| High Value Spike | > 2,000,000 VND / 60s | Ban alert `wallet-fraud-alerts` |

### Write-Behind DB Sync

```
Redis Lua Script -> XADD wallet:stream:db_sync_queue
                              │
                              ▼
                    DbBatchSyncWorker (poll 2s)
                              │
                              ▼
                    PostgreSQL (Bulk Upsert)
```

---

## Bien moi hoi / Cau hinh

| Bien moi hoi | Mo ta | Default |
|-------------|-------|---------|
| `POD_NAME` | Instance ID cho Static Group Membership | `wallet-consumer-instance-xxxx` |
| `KAFKA_BOOTSTRAP` | Bootstrap servers | `localhost:9092` |
| `REDIS_CONNECTION` | Redis connection string | `localhost:6379` |
| `OPENSEARCH_URL` | OpenSearch endpoint | `http://localhost:9200` |

---

## Yeu cau

- .NET SDK 8.0+
- Docker + Docker Compose
- 4GB+ RAM (cho Kafka 3 brokers + OpenSearch)
