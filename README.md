
# Real-Time ML Feature Store & Dynamic Recommendation Pipeline

An end-to-end, low-latency machine learning feature store and candidate-ranking engine designed to eliminate feature drift and training-serving skew in real-time recommendation systems.

The architecture decouples operational event ingestion from offline analytical logging using a dual-storage paradigm (PostgreSQL 16 + Redis 7). It implements non-blocking asynchronous event offloading, exponential recency decay, online drift detection, and sub-75ms vector similarity inference under heavy concurrency.

---

## System Architecture

```
                         +-----------------------------------------------+
                         |           FastAPI Gateway (Uvicorn)           |
                         +-----------------------+-----------------------+
                                                 |
                                [POST /events Ingestion]
                                                 |
                                                 v
                                 Non-Blocking Background Worker
                                                 |
                       +-------------------------+-------------------------+
                       |                                                   |
                       v                                                   v
        +-----------------------------+                     +-----------------------------+
        |   Online Store (Redis 7)    |                     |  Offline Store (PostgreSQL) |
        |  - Sub-2ms RAM Retrieval    |                     |  - Immutable Event Logs     |
        |  - Capped Sliding Lists     |                     |  - Point-in-Time Training   |
        |  - Decayed Category Weights |                     |  - Composite B-Tree Index   |
        +--------------+--------------+                     +-----------------------------+
                       |
             Live Feature Vector
                       |
                       v
        +-----------------------------+
        |    NumPy Inference Engine   |
        |  - Vectorized Cosine Sim    |
        |  - Candidate Ranking        |
        |  - Recency De-duplication   |
        +--------------+--------------+
                       |
              [GET /recommend/{id}]

```

### Core Engineering Principles

1. **Dual-Tier Storage (Decoupled Writes):**
* **Online Store (Redis 7):** Serves user state and decayed category weights in RAM with sub-millisecond retrieval latency.
* **Offline Store (PostgreSQL 16):** Stores raw, timestamped interaction logs for offline model training and historical auditing.
* **Decoupled Workers:** Write operations to both databases run through FastAPI BackgroundTasks, allowing `POST /events` to respond immediately with an HTTP 202 (Accepted) in under 3ms.


2. **Exponential Recency Decay (lambda = 0.85):**
* Eliminates the bias of stale historical interactions without requiring periodic, expensive batch recomputations.
* On every new event, historical category affinity scores scale down (S_t = S_{t-1} * 0.85), while the active interaction receives +1.0.


3. **Online Feature Drift Detection:**
* During event ingestion, the pipeline compares `old_primary_category` against `new_primary_category`.
* When an interest shift is detected, an alert is recorded and the Redis pointer `user:{id}:primary_category` is updated immediately.


4. **Vectorized SIMD Candidate Ranking:**
* The inference route scores an item catalog against a user's live category affinity weights using pure matrix-vector cosine similarity in NumPy.
* Incorporates recency deduplication by discounting items the user already interacted with in their sliding click window (0.2x multiplier).



---

## Concurrency Benchmarks (Locust)

The system was benchmarked using Locust under 50 concurrent simulated users issuing a 70/30 write/read traffic split (`POST /events` ingestion vs. `GET /recommend` vector inference) for 30 seconds:

```text
Type     Name                            # reqs      Avg     Min     Max     Med |      req/s
--------|----------------------------|---------|-------|-------|-------|---------|-----------
POST     /events [POST Ingestion]          6129      25       2     242      24 |     215.73
GET      /recommend [GET Inference]        2522      28       4     131      27 |      88.77
--------|----------------------------|---------|-------|-------|-------|---------|-----------
         Aggregated                        8651      26       2     242      25 |     304.50

```

### Latency Percentiles (Target vs. Actual)

| Endpoint | 50th % (Median) | 90th % | 95th % | 99th % | Target SLA |
| --- | --- | --- | --- | --- | --- |
| **POST /events** | **24 ms** | 41 ms | **48 ms** | **72 ms** | < 100 ms |
| **GET /recommend** | **27 ms** | 45 ms | **51 ms** | **73 ms** | < 100 ms |
| **Aggregated Total** | **25 ms** | 42 ms | **49 ms** | **72 ms** | < 100 ms |

* **Total Throughput:** Sustained 304.5 requests/second on a single instance.
* **Failure Rate:** 0.00% (0 failed requests out of 8,651).

---

## Tech Stack

* **Language & API:** Python 3.11+, FastAPI, Uvicorn, Pydantic V2
* **Data Stores:** Redis 7 (In-Memory Feature Cache), PostgreSQL 16 (Analytical Warehouse)
* **Async Drivers:** aioredis, asyncpg (leveraging binary COPY protocol)
* **Vector Math:** NumPy (Vectorized Cosine Similarity)
* **Infrastructure & Load Testing:** Docker Compose, Locust

---

## Repository Structure

```text
├── docker-compose.yml       # PostgreSQL 16 & Redis 7 container configuration
├── schema.sql               # Relational DDL & performance indexing
├── seed_data.py             # High-speed bulk event generation (asyncpg COPY)
├── warmup_redis.py          # Batch ETL populating Redis online feature store
├── main.py                  # FastAPI service (ingestion, drift detector, inference)
├── verify_live.py           # Verification script for dual-storage updates
├── locustfile.py            # High-concurrency performance benchmark suite
├── requirements.txt         # Pinned Python package dependencies
├── .gitignore               # Ignored runtime artifacts
└── README.md                # Project documentation

```

---

## Quickstart Guide

### 1. Clone & Setup Virtual Environment

```bash
git clone https://github.com/harshilsiddharth27/realtime-rec-feature-store.git
cd realtime-rec-feature-store

python -m venv venv
# Windows:
.\venv\Scripts\Activate.ps1
# Linux/macOS:
source venv/bin/activate

pip install -r requirements.txt

```

### 2. Launch Storage Infrastructure

Start PostgreSQL and Redis via Docker Compose:

```bash
docker compose up -d

```

### 3. Initialize Warehouse & Warmup Cache

Run the database migrations and generate baseline data:

```bash
python seed_data.py

```

Warm up the Redis feature cache with recent interactions from PostgreSQL:

```bash
python warmup_redis.py

```

### 4. Start the Application Server

```bash
uvicorn main:app --reload --port 8000

```

Interactive OpenAPI documentation will be available at: `[http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)`.

---

## API Reference

### 1. Ingest User Event

* **Endpoint:** `POST /events`
* **Status Code:** `202 Accepted`

```json
{
  "user_id": 1,
  "item_id": 301,
  "category": "fitness",
  "interaction_type": "click"
}

```

### 2. Inspect Real-Time Features

* **Endpoint:** `GET /features/{user_id}`
* **Status Code:** `200 OK`

```json
{
  "user_id": 1,
  "recent_clicks": ["301", "101", "102"],
  "category_affinity_scores": {
    "gaming": 1.2541,
    "fitness": 2.1485
  },
  "primary_category": "fitness"
}

```

### 3. Candidate Ranking & Recommendations

* **Endpoint:** `GET /recommend/{user_id}?top_k=3`
* **Status Code:** `200 OK`

```json
{
  "user_id": 1,
  "strategy": "real_time_vector_similarity",
  "top_recommendations": [
    {
      "item_id": 302,
      "name": "Adjustable Dumbbells",
      "category": "fitness",
      "similarity_score": 0.8642,
      "recently_viewed": false
    },
    {
      "item_id": 101,
      "name": "Mechanical Keyboard",
      "category": "gaming",
      "similarity_score": 0.1417,
      "recently_viewed": true
    }
  ]
}

```

---

## Running the Concurrency Benchmark

To replicate the 50-user load test locally:

```bash
locust -f locustfile.py --headless -u 50 -r 10 --run-time 30s --host http://127.0.0.1:8000

```
