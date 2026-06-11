# Kafka Core: Real-Time Observability & AI Platform

**A production-grade event-driven monitoring and analytics system combining Kafka, ML, vector databases, and LLM-powered insights.**

## Table of Contents
1. [Project Overview](#project-overview)
2. [Current State (Phase 1)](#current-state-phase-1)
3. [Two-Node Architecture](#two-node-architecture)
4. [Complete Implementation Roadmap](#complete-implementation-roadmap)
5. [Phase-by-Phase Setup Guide](#phase-by-phase-setup-guide)
6. [Learning Path](#learning-path)

---

## Project Overview

### What is This Project?

This project is a **real-time event-driven data platform** that:
- Collects system, container, and application metrics from one or more sources
- Streams them through Apache Kafka
- Processes them with stateless consumers
- Stores processed data in PostgreSQL and InfluxDB
- Exposes metrics via Grafana dashboards
- Uses ML for anomaly detection
- Leverages LLMs and vector databases for intelligent insights
- Provides GraphQL and REST APIs for flexible querying

### Core Philosophy

```
Data Collection (Node A)
    ↓
Kafka Message Bus (Node B)
    ↓
Consumer Processing (Node B)
    ↓
Storage Layer (Node B)
    ↓
Visualization & AI APIs (Node B)
```

**Key Principle**: Node A (Dell Server) only collects and publishes. All processing, storage, and AI happens on Node B (MacBook Air).

### Why Build This?

- **Learning Real-Time Systems**: Master Kafka, event streaming, and distributed architecture
- **Production-Grade Monitoring**: Go beyond simple dashboards into intelligent alerting
- **AI Integration**: Learn how to combine LLMs with operational data for explanations
- **Vector Databases**: Understand semantic search and embeddings in a practical domain
- **GraphQL**: Flexible API layer for frontend applications

---

## Current State (Phase 1)

### ✅ What's Implemented

| Component | Status | Location | Purpose |
|-----------|--------|----------|---------|
| Kafka Broker | ✅ Running | Node B (192.168.2.110:9092) | Central event hub |
| Host Metrics Producer | ✅ Running | Node A | System uptime, load, process count |
| Disk Metrics Producer | ✅ Running | Node A | Disk usage, I/O stats |
| Container Metrics Producer | ✅ Running | Node A | Docker container performance |
| Process Metrics Producer | ✅ Running | Node A | Individual process stats |
| Network Metrics Producer | 🚧 Partial | Node A | Network telemetry (planned) |

### ❌ What's Planned

| Component | Phase | Location | Purpose |
|-----------|-------|----------|---------|
| ETL Consumer | Phase 1 | Node B | Data cleaning, normalization |
| Analytics Consumer | Phase 1 | Node B | Rolling averages, spike detection |
| ML Anomaly Consumer | Phase 1 | Node B | Isolation Forest anomaly detection |
| AI Consumer | Phase 1 | Node B | LLM integration |
| PostgreSQL | Phase 1 | Node B | Structured data storage |
| InfluxDB | Phase 1 | Node B | Time-series storage |
| Grafana | Phase 1 | Node B | Visualization & dashboards |
| Vector Database | Phase 2 | Node B | Semantic search (Qdrant/Pinecone) |
| Embeddings Service | Phase 2 | Node B | Convert metrics to vectors |
| GraphQL API | Phase 2 | Node B | Flexible metric queries |
| Chatbot Service | Phase 2 | Node B | Natural language interface |

---

## Two-Node Architecture

### Node A: Dell Server (Data Source Node)

**Hardware**: Intel i5-3450, 8GB RAM, multiple HDDs  
**Role**: Metric collection and publishing only  
**IP**: 192.168.x.x

#### Active Producers
1. **Host Metrics** (`host_metrics.py`)
   - CPU cores, load average
   - Process count
   - System uptime
   - Published to: `host.metrics`

2. **Disk Metrics** (`disk_metrics.py`)
   - Total/used/free disk space
   - Disk I/O stats (read/write bytes and operations)
   - Published to: `system.disk.metrics`

3. **Container Metrics** (`container_metrics.py`)
   - CPU usage per container
   - Memory usage and percentage
   - Container status
   - Network RX stats (partial)
   - Published to: `container.metrics`

4. **Process Metrics** (`process_metrics.py`)
   - Individual process CPU/memory
   - Published to: `process.metrics`

5. **Network Metrics** (`network_metrics.py`)
   - Network interface stats
   - Published to: `network.metrics` (planned)

#### Producer Design Pattern
```python
# All producers follow this pattern:
1. Collect raw metrics from psutil or Docker API
2. Format as JSON with schema:
   {
     "event_type": "host.metrics",
     "timestamp": "2026-06-11T10:00:00Z",
     "host": "dell-node-a",
     "metrics": { ... },
     "tags": { "env": "lab", "node": "dell-node-a" }
   }
3. Send to Kafka broker (192.168.2.110:9092)
4. Repeat every N seconds (configurable)
```

**Important**: Node A does NOT:
- Store any data
- Run ML models
- Perform aggregations
- Run Kafka broker

### Node B: MacBook Air Ubuntu (Core Platform Node)

**Hardware**: Intel i5-5350U (2C/4T), 8GB RAM, SSD  
**Role**: Central data platform for all processing and storage  
**IP**: 192.168.2.110

#### Layer 1: Event Ingestion
- **Kafka Broker**: Receives all events from Node A on port 9092
  - Acts as central message bus
  - Provides decoupling between producers and consumers
  - Stores events for configurable retention period

#### Layer 2: Consumer Processing (Phase 1)
All consumers run on Node B and independently process Kafka topics:

1. **ETL Consumer**
   - Validates event schema
   - Cleans and normalizes data
   - Publishes clean events to `metrics.normalized` topic
   - Writes to PostgreSQL

2. **Analytics Consumer**
   - Computes rolling averages (5-min, 1-hour, 24-hour windows)
   - Detects spikes and anomalies
   - Publishes to `metrics.analytics` topic
   - Writes to InfluxDB for time-series data

3. **ML Anomaly Consumer**
   - Trains Isolation Forest on metric streams
   - Assigns anomaly scores (0-1) to each metric point
   - Publishes anomalies to `anomalies.detected` topic
   - Stores predictions in PostgreSQL

4. **AI Consumer** (Phase 1)
   - Fetches context from PostgreSQL and InfluxDB
   - Calls Ollama LLM for explanations
   - Answers questions like: "Why is CPU high?"

#### Layer 3: Storage Layer
- **PostgreSQL** (port 5432)
  - Structured data: metrics, anomalies, metadata
  - Features for ML models
  - Used by AI layer for context

- **InfluxDB** (port 8086)
  - Time-series metrics
  - Optimized for high-write, high-query workloads
  - Used by Grafana for dashboards

#### Layer 4: Visualization
- **Grafana** (port 3000)
  - Dashboards for system health
  - Real-time metric visualization
  - Anomaly alerts

- **Ollama** (port 11434)
  - Local LLM inference (Llama2, Mistral, etc.)
  - Used by AI consumer for explanations

#### Layer 5: AI Services (Phase 2+)
- **Vector Database** (port 6333)
  - Stores semantic embeddings of metrics and logs
  - Enables "find similar incidents" queries

- **Embeddings Service** (port 8002)
  - Converts metric summaries to vector embeddings
  - Uses HuggingFace or OpenAI models

- **GraphQL API** (port 8000)
  - Flexible queries: metrics, anomalies, semantic search
  - Used by frontend applications

- **Chatbot Service** (port 8001)
  - Natural language interface
  - Uses LangChain + LLM
  - Answers monitoring questions

---

## Complete Implementation Roadmap

### Phase 1: Core Event Pipeline (Weeks 1-2)

**Goal**: Get data flowing end-to-end from producers → Kafka → storage → visualization

#### Phase 1a: Storage & Databases (1-2 days)
**Why**: Producers are running; now we need to store their data

**Implementation**:
1. Install PostgreSQL & InfluxDB
2. Create schema migration scripts (`migrations/001_initial_schema.sql`)
3. Set up retention policies and bucket configurations

**Deliverable**: Empty but properly-schemed databases

---

#### Phase 1b: ETL Consumer (2-3 days)
**Why**: Validate and clean raw data before storage

**Implementation**:
1. Create consumer structure with schema validation
2. Implement data normalization logic
3. Add PostgreSQL writer module with batch processing
4. Add health check endpoint `/health`

**What You'll Learn**:
- Kafka consumer group patterns
- Event schema validation
- Database connection pooling
- Error handling and dead letter queues

**Deliverable**: Working consumer writing validated data to PostgreSQL

---

#### Phase 1c: Analytics Consumer (2-3 days)
**Why**: Compute aggregates and detect spikes in real-time

**Implementation**:
1. Build rolling window aggregators (5-min, 1-hour, 24-hour)
2. Implement spike detection (outlier > avg + 2*std_dev)
3. Create InfluxDB writer with line protocol
4. Add time-series data retention management

**What You'll Learn**:
- Time-series database concepts
- Windowing and aggregation patterns
- Statistical spike detection

**Deliverable**: Time-series data in InfluxDB; spike detection working

---

#### Phase 1d: ML Anomaly Detector (3-4 days)
**Why**: ML-based anomaly detection learns patterns over time

**Implementation**:
1. Implement Isolation Forest model using scikit-learn
2. Create daily retraining pipeline
3. Store model artifacts and predictions in PostgreSQL
4. Expose anomaly scores via queryable interface

**What You'll Learn**:
- Isolation Forest algorithm and hyperparameters
- Model training and inference patterns
- Feature engineering for time-series data

**Deliverable**: ML anomalies detected and scored; models persist

---

#### Phase 1e: Grafana Dashboards (1-2 days)
**Why**: Visualize all collected data in real-time

**Implementation**:
1. Connect PostgreSQL and InfluxDB as data sources
2. Create dashboards for: system health, containers, anomalies, resources
3. Add alert rules and notifications
4. Export dashboard JSON for version control

**Deliverable**: Live dashboards at `localhost:3000`

---

#### Phase 1f: AI Consumer (LLM) (2-3 days)
**Why**: Natural language explanations for anomalies

**Implementation**:
1. Install Ollama and download Llama2 model
2. Build context aggregator (fetch related metrics)
3. Implement LLM prompt engineering
4. Store explanations alongside anomalies

**Deliverable**: Anomalies have LLM-generated explanations

---

### Phase 2: AI & Vector Database (Weeks 3-4)

**Goal**: Add semantic search, embeddings, GraphQL API, and chatbot

#### Phase 2a: Vector Database (1 day)
**Why**: Enable semantic search across metrics

**Implementation**:
1. Deploy Qdrant with Docker: `docker run -p 6333:6333 qdrant/qdrant`
2. Create collections for metrics and anomalies (384-dim vectors)
3. Set up metadata filtering

**Deliverable**: Vector DB running with collections

---

#### Phase 2b: Embeddings Service (2-3 days)
**Why**: Convert metrics/logs to semantic vectors

**Implementation**:
1. Build FastAPI service with HuggingFace sentence-transformers
2. Create `/embed` and `/embed_batch` endpoints
3. Add model caching for performance
4. Document payload format and API contract

**What You'll Learn**:
- Embeddings fundamentals (what they represent)
- FastAPI async patterns
- Model inference and batching

**Deliverable**: Service at `localhost:8002` generating embeddings

---

#### Phase 2c: Embeddings Consumer (2 days)
**Why**: Continuously index metrics for semantic search

**Implementation**:
1. Create Kafka consumer for normalized metrics
2. Batch events and send to embeddings service
3. Store vectors + metadata in Qdrant
4. Handle duplicates and updates

**Deliverable**: Metrics indexed in vector database

---

#### Phase 2d: GraphQL API (3-4 days)
**Why**: Flexible API for querying metrics and anomalies

**Implementation**:
1. Design schema with Strawberry (Metric, Anomaly, SemanticSearchResult types)
2. Implement resolvers for each query type
3. Query PostgreSQL, InfluxDB, and Qdrant from resolvers
4. Add filtering, pagination, and sorting
5. Document queries with examples

**What You'll Learn**:
- GraphQL schema design
- Query optimization with database indexing
- Resolver composition patterns

**Deliverable**: GraphQL API at `localhost:8000` serving queries

---

#### Phase 2e: Chatbot Service (3-4 days)
**Why**: Natural language interface for complex queries

**Implementation**:
1. Define LangChain tools: query_postgres, semantic_search, get_context
2. Set up ReAct agent with Ollama LLM
3. Create FastAPI endpoints `/chat` and `/chat/history`
4. Add conversation memory and context management

**What You'll Learn**:
- LangChain agent patterns and tool use
- Prompt engineering for Reasoning + Acting (ReAct)
- Conversation state management

**Deliverable**: Chatbot at `localhost:8001` answering questions

---

### Phase 3: Production & Deployment (Week 4+)

#### Phase 3a: Docker Compose
Complete multi-service orchestration in one command

#### Phase 3b: CI/CD Pipeline
Automated testing with GitHub Actions

#### Phase 3c: Monitoring & Observability
Health checks, consumer lag, performance metrics

---

## Phase 4+: Advanced AI Topics

Beyond the core features, here are powerful AI capabilities you can add:

### 🔥 RAG (Retrieval-Augmented Generation) — HIGHEST IMPACT

**What it does**: When an anomaly is detected, automatically find similar past incidents and their resolutions, then use that context to generate better explanations and fix recommendations.

**Real Example**:
```
Anomaly: Memory spike in container-api
System: Searches vector DB for similar incidents
Found: 3 past memory spikes (2026-06-01, 2026-05-15, 2026-04-20)
Context: All 3 were caused by Java garbage collection issues
Resolution: Update JVM heap settings + add memory monitoring
Output: "Memory spike detected (similar to Jun 1). Likely cause: GC tuning issue. 
         Recommended fix: Increase Xmx to 4GB and add CMS garbage collector."
```

**Implementation** (3-4 days):
1. Create `incidents` table: store past incidents + resolutions + tags
2. Build `rag_consumer.py` that triggers on anomalies
3. Query vector DB for similar incidents (semantic similarity)
4. Pass incident context + current anomaly to LLM
5. Generate actionable recommendations

**Learning**: Production RAG patterns, context window management, retrieval quality

---

### Root Cause Analysis (RCA) Engine

**What it does**: Automatically trace anomalies backward to their root causes by analyzing metric correlations and dependency graphs.

**Real Example**:
```
Observed: High CPU on container-web
System traces: CPU ← Slow queries ← High memory on container-db ← Disk I/O
Root Cause: Database indices not loading due to disk space
Fix: Clean up old logs (freed 50GB), indices loaded, queries fast, CPU normal
```

**Implementation** (4-5 days):
1. Build correlation matrix between metrics (Pearson/Spearman)
2. Create dependency graph (which metric causes which)
3. Use Granger causality for time-series causation
4. Trace anomaly backward through dependency graph
5. Return ranked list of potential root causes

**Advanced**: Use causal inference frameworks (DoWhy library)

**Learning**: Causal inference, dependency graphs, time-series causality analysis

---

### Time-Series Forecasting

**What it does**: Predict metric values 1-24 hours ahead. Alert before problems happen (e.g., "Disk will fill in 6 hours").

**Model Options** (progressive complexity):

**Simple** (1 day):
- ARIMA/SARIMA - statistical baseline
- Good for: stable, seasonal patterns

**Medium** (2-3 days):
- Facebook Prophet - handles holidays, changepoints
- Good for: business metrics with clear seasonality

**Advanced** (1 week):
- LSTMs - learns complex temporal patterns
- Transformer models - state-of-the-art, handles multiple inputs

**Implementation**:
1. Train separate model per metric
2. Validate with walk-forward testing
3. Expose predictions via GraphQL
4. Add alerts when forecast crosses threshold

**Chatbot integration**: "Will we hit 90% disk in 8 hours?" → checks forecast → "Yes, in 7.5 hours"

**Learning**: ARIMA, Prophet, LSTMs, hyperparameter tuning

---

### Model Explainability (SHAP / LIME)

**What it does**: Answer "Why did the ML model flag this as an anomaly?"

**Example Output**:
```
Anomaly Score: 0.87 (Very Anomalous)

Contributing Factors:
- Memory usage 450MB ↑↑↑ (pushed score +0.35)
- Disk I/O 8500 ops/s ↑↑ (pushed score +0.28)
- CPU 12% ↑ (pushed score +0.24)
- Network RX 2.5MB/s (normal, contribution 0)
```

**Implementation** (2-3 days):
1. Use SHAP for global + local explanations
2. Generate per-anomaly feature importance
3. Create explanation service
4. Expose via `/explain` endpoint + chatbot

**Learning**: SHAP theory, model interpretability, feature importance

---

### Multi-Agent Systems

**What it does**: Specialize agents for different problem domains (CPU expert, memory expert, disk expert). Router agent picks the right one.

**Architecture**:
```
User: "Why is the system slow?"
           ↓
     Router Agent (LLM)
     ↓         ↓          ↓
CPU Agent  Memory Agent  Disk Agent  (each specializes)
     ↓         ↓          ↓
  Analyzes metrics & historical patterns for their domain
     ↓         ↓          ↓
Router synthesizes responses → "Slow system is due to
high disk I/O (found by disk agent) caused by memory
pressure (found by memory agent). Fix: add more RAM."
```

**Implementation** (4-5 days):
1. Create specialized agents per domain
2. Give each agent domain-specific tools
3. Create router agent with LLM
4. Route queries to appropriate specialist
5. Synthesize responses

**Learning**: Multi-agent architecture, agent routing, domain specialization

---

### Automated Runbook Generation

**What it does**: Generate step-by-step troubleshooting guides automatically when anomalies occur.

**Example Output**:
```markdown
# High CPU Anomaly Response Guide
**Timestamp**: 2026-06-11 14:30 UTC
**Severity**: Medium

## Step 1: Check Running Processes
$ ps aux | sort -k3 -r | head -5
Expected: Identify top CPU consumer

## Step 2: Profile the Process
$ perf record -p <PID> -F 99 sleep 10
Look for: Expensive functions in hot path

## Step 3: Check History
Similar incidents:
- 2026-06-05 14:20 (Resolved by: restart container)
- 2026-06-01 13:00 (Resolved by: apply patch 2.3.1)
```

**Implementation** (3-4 days):
1. Template library of troubleshooting steps per anomaly type
2. LLM generates markdown runbooks
3. Integrate with RAG to fetch historical resolutions
4. Store runbooks for documentation

**Learning**: Structured LLM outputs, domain knowledge encoding

---

### Natural Language Log Analysis

**What it does**: Parse unstructured logs alongside metrics to correlate with anomalies.

**Real Example**:
```
Anomaly: Memory spike at 14:25
Logs at 14:25:
- ERROR: OutOfMemoryError in JVM (container-api)
- WARN: GC overhead limit exceeded

System links: Memory spike + OOM error = Java garbage collection issue
```

**Implementation** (3-4 days):
1. Log parser with grok patterns or regex
2. NER to extract error types, codes, services
3. Embed logs into vector DB
4. Correlate log embeddings with metric anomalies
5. Show logs alongside anomalies in Grafana

**Learning**: NLP on logs, zero-shot classification, structured extraction

---

### Few-Shot Learning

**What it does**: Teach the system about new anomaly types with just a few examples.

**Example**:
```
User: "This is a 'database deadlock' anomaly:
Example 1: High lock wait time + query timeout
Example 2: Multiple queries stuck on same table
Example 3: Sudden spike in CPU + disk I/O

Is this new incident also a database deadlock?"

System: Uses few examples in-context to classify correctly
```

**Implementation** (2-3 days):
1. Prompt engineering for in-context learning
2. Build example library per anomaly type
3. Create classification prompt that includes examples
4. User can add new examples dynamically

**Learning**: In-context learning, prompt engineering, zero/few-shot patterns

---

### Knowledge Graphs

**What it does**: Model relationships between metrics, containers, services, and incidents. Enables powerful queries.

**Example Queries**:
```
"Show me all containers affected by database-api failure"
"What services depend on this disk?"
"Find the blast radius of this outage"
```

**Implementation** (4-6 days):
1. Use Neo4j or networkx for graph
2. Build graph builder: services → containers → metrics → incidents
3. Query service: graph traversal for relationships
4. Visualize: show dependency trees

**Learning**: Graph databases, graph queries, dependency modeling

---

### AutoML for Metric Prediction

**What it does**: Automatically select the best forecasting model for each metric.

**How it works**:
1. Train 5 models (ARIMA, Prophet, LSTM, XGBoost, Ensemble) on each metric
2. Validate on holdout set (RMSE, MAE, MAPE)
3. Pick the best model per metric
4. Monitor performance; switch if accuracy drops

**Implementation** (4-5 days):
1. Create model zoo with pluggable models
2. Trainer that tries all models
3. Scorer and selector
4. Online learning to adapt to distribution shift

**Learning**: Model selection, cross-validation, ensemble methods

---

### Anomaly Summarization

**What it does**: Generate executive summaries of system behavior (daily, weekly, monthly).

**Example Output**:
```markdown
## Week Summary: Jun 2-8, 2026

**Reliability**: 98.7% (↑0.3% vs previous week)

**Anomalies**: 24 detected (↓12%)
- Top Issue: Memory leaks in container-nginx (8 occurrences)
  - Root Cause: Identified [link to analysis]
  - Action Taken: Memory limits increased; monitoring added
  - Status: Resolved

**Performance Trends**:
- Average response time: 145ms (↓8%)
- 95th percentile latency: 250ms (stable)
- Database queries: avg 23ms (↓5%)

**Recommended Actions**:
1. Update Nginx config (see runbook #42)
2. Add CPU headroom to container-api
```

**Implementation** (2-3 days):
1. Aggregate metrics over time windows
2. Extract key events (anomalies, deployments)
3. LLM generates narrative summary
4. Link to detailed analysis

**Learning**: Abstractive summarization, executive reporting

---

## Roadmap Matrix

| Feature | Phase | Effort | Impact | Learning Value |
|---------|-------|--------|--------|-----------------|
| RAG (Incident Context) | 4 | 3-4 days | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ |
| Root Cause Analysis | 4 | 4-5 days | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| Time-Series Forecasting | 4 | 3-7 days | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| Model Explainability | 4 | 2-3 days | ⭐⭐⭐⭐ | ⭐⭐⭐ |
| Multi-Agent Systems | 4 | 4-5 days | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| Automated Runbooks | 4 | 3-4 days | ⭐⭐⭐ | ⭐⭐⭐ |
| Log Analysis | 4 | 3-4 days | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ |
| Few-Shot Learning | 4 | 2-3 days | ⭐⭐⭐ | ⭐⭐⭐⭐ |
| Knowledge Graphs | 4 | 4-6 days | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ |
| AutoML | 4 | 4-5 days | ⭐⭐⭐ | ⭐⭐⭐⭐ |

---

## Recommended Phase 4 Priority

**Start with these (highest ROI)**:
1. **RAG** - Transforms chatbot from "explains anomaly" to "provides solutions"
2. **Time-Series Forecasting** - Enables proactive alerting
3. **Root Cause Analysis** - Dramatically improves incident response

**Then add**:
4. Model Explainability - Understand ML decisions
5. Multi-Agent Systems - Organize knowledge by domain
6. Log Analysis - Get more signal from logs

---

## Phase-by-Phase Setup Guide

### Prerequisites

**All Nodes**:
- Python 3.10+
- Git

**Node A**:
- psutil
- docker-py
- kafka-python

**Node B**:
- Kafka (already running)
- PostgreSQL
- InfluxDB
- Grafana
- Ollama

### Quick Start (Phase 1)

```bash
# Clone and setup
git clone https://github.com/your-org/kafka-core
cd kafka-core
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Set up databases
createdb kafka_metrics
psql kafka_metrics < migrations/001_initial_schema.sql

# Run consumers (in separate terminals)
python consumers/etl_consumer.py
python consumers/analytics_consumer.py
python consumers/ml_consumer.py
python consumers/ai_consumer.py

# Access Grafana
open http://localhost:3000
```

### Quick Start (Phase 2)

```bash
# Set up vector DB
docker run -p 6333:6333 qdrant/qdrant

# Start AI services (in separate terminals)
python services/embeddings_service/main.py
python services/graphql_api/main.py
python services/chatbot/main.py

# Test GraphQL
open http://localhost:8000
```

---

## Learning Path

### Key Concepts by Phase

#### Phase 1
- Kafka consumer groups and offset management
- Event schema validation and normalization
- Time-series database fundamentals
- ML anomaly detection (Isolation Forest)
- LLM prompt engineering

#### Phase 2
- Embeddings and vector spaces
- Vector database indexing (HNSW)
- GraphQL schema design and resolvers
- LangChain agents and tool use
- Semantic search ranking

#### Phase 3
- Docker containerization and orchestration
- CI/CD pipelines and testing
- Distributed system monitoring
- Performance optimization

### Recommended Resources

**Kafka**: "Kafka: The Definitive Guide" by Narkhede, Shapira, Palino  
**Vector DB**: Qdrant docs (https://qdrant.tech/documentation/)  
**GraphQL**: Official GraphQL docs (https://graphql.org/)  
**LLMs**: LangChain docs (https://python.langchain.com/)  

---

## Questions & Common Issues

**Q: Why separate nodes?**  
A: Mimics production systems. Node A is lightweight edge; Node B is powerful core.

**Q: Can I run everything on one machine?**  
A: Yes! Change producer connection to `localhost:9092`.

**Q: Which vector DB should I use?**  
A: Qdrant (learning), Pinecone (production), Milvus (enterprise).

---

## Next Steps

1. **This Week**: Set up Phase 1a (databases) and Phase 1b (ETL consumer)
2. **Next Week**: Complete Phase 1 (analytics, ML, AI, Grafana)
3. **Week 3**: Implement Phase 2 (vector DB, embeddings, GraphQL)
4. **Week 4**: Add chatbot and production deployment

**Questions?** Open an issue or start a discussion!

---

## Reference: Current Architecture

### Produced Topics
- `host.metrics`: System uptime, load, processes
- `system.disk.metrics`: Disk usage and I/O
- `container.metrics`: Docker container stats
- `process.metrics`: Individual process metrics
- `network.metrics`: Network telemetry (planned)

### Planned Internal Topics (Phase 1+)
- `metrics.normalized`: Cleaned events
- `metrics.analytics`: Aggregated stats
- `anomalies.detected`: ML-scored anomalies
- `embeddings.generated`: Vector embeddings

### Services & Ports
- Kafka: 9092
- PostgreSQL: 5432
- InfluxDB: 8086
- Grafana: 3000
- Ollama: 11434
- Qdrant: 6333 (Phase 2)
- Embeddings: 8002 (Phase 2)
- GraphQL: 8000 (Phase 2)
- Chatbot: 8001 (Phase 2)
