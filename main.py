from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Literal, List, Dict
from fastapi import FastAPI, BackgroundTasks, Request, Query
from pydantic import BaseModel, Field
import redis.asyncio as aioredis
import asyncpg
import numpy as np
import time

# 1. Incoming Event Contract
class UserEvent(BaseModel):
    user_id: int = Field(..., gt=0, description="Target user ID")
    item_id: int = Field(..., gt=0, description="ID of item interacted with")
    category: str = Field(..., min_length=2, max_length=50, description="Item category")
    interaction_type: Literal["view", "click", "purchase"] = Field(default="click")

# 2. Storage Credentials & Fixed Category Dimensions
POSTGRES_DSN = "postgresql://feature_user:feature_password@localhost:5432/feature_warehouse"
REDIS_URL = "redis://localhost:6379/0"
DECAY_FACTOR = 0.85

ORDERED_CATEGORIES = ["electronics", "apparel", "home", "books", "gaming", "fitness"]
CAT_TO_INDEX = {cat: i for i, cat in enumerate(ORDERED_CATEGORIES)}

# 3. Static Item Catalog (Candidate Set for Scoring)
# In enterprise systems, this resides in Postgres/Milvus/Qdrant
ITEM_CATALOG = [
    {"item_id": 101, "name": "Mechanical Keyboard", "category": "gaming"},
    {"item_id": 102, "name": "Wireless Mouse", "category": "gaming"},
    {"item_id": 103, "name": "4K Gaming Monitor", "category": "gaming"},
    {"item_id": 201, "name": "Noise Cancelling Headphones", "category": "electronics"},
    {"item_id": 202, "name": "USB-C Fast Charger", "category": "electronics"},
    {"item_id": 301, "name": "Running Shoes", "category": "fitness"},
    {"item_id": 302, "name": "Adjustable Dumbbells", "category": "fitness"},
    {"item_id": 401, "name": "System Design Handbook", "category": "books"},
    {"item_id": 402, "name": "Clean Code", "category": "books"},
    {"item_id": 501, "name": "Cotton Crewneck T-Shirt", "category": "apparel"},
    {"item_id": 601, "name": "Ergonomic Desk Lamp", "category": "home"}
]

# Precompute item category vectors (One-Hot Encoded Matrix: N x D)
def build_item_matrix():
    matrix = np.zeros((len(ITEM_CATALOG), len(ORDERED_CATEGORIES)), dtype=np.float32)
    for row_idx, item in enumerate(ITEM_CATALOG):
        if item["category"] in CAT_TO_INDEX:
            matrix[row_idx, CAT_TO_INDEX[item["category"]]] = 1.0
    return matrix

ITEM_VECTORS = build_item_matrix()

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("[*] Initializing database connection pools...")
    app.state.pg_pool = await asyncpg.create_pool(
        dsn=POSTGRES_DSN,
        min_size=2,
        max_size=10
    )
    app.state.redis = aioredis.from_url(REDIS_URL, decode_responses=True)
    yield
    print("[*] Draining and closing database connections...")
    await app.state.pg_pool.close()
    await app.state.redis.aclose()

app = FastAPI(
    title="Real-Time ML Feature Store Pipeline",
    version="1.2.0",
    lifespan=lifespan
)

@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.perf_counter()
    response = await call_next(request)
    process_time_ms = (time.perf_counter() - start_time) * 1000
    response.headers["X-Process-Time-Ms"] = f"{process_time_ms:.2f}"
    return response

# 4. Background Ingestion Worker
async def process_event_background(event: UserEvent, pg_pool: asyncpg.Pool, r: aioredis.Redis):
    try:
        clicks_key = f"user:{event.user_id}:recent_clicks"
        scores_key = f"user:{event.user_id}:category_scores"
        drift_key = f"user:{event.user_id}:primary_category"

        current_scores = await r.hgetall(scores_key)
        old_primary = max(current_scores, key=lambda k: float(current_scores[k])) if current_scores else None

        decayed_scores = {}
        for cat, val in current_scores.items():
            decayed_scores[cat] = round(float(val) * DECAY_FACTOR, 4)
        
        decayed_scores[event.category] = round(decayed_scores.get(event.category, 0.0) + 1.0, 4)
        new_primary = max(decayed_scores, key=lambda k: decayed_scores[k])

        pipe = r.pipeline()
        pipe.lpush(clicks_key, event.item_id)
        pipe.ltrim(clicks_key, 0, 4)
        pipe.delete(scores_key)
        pipe.hset(scores_key, mapping=decayed_scores)

        if old_primary is not None and old_primary != new_primary:
            print(f"[ALERT] Feature Drift for User {event.user_id}: '{old_primary}' -> '{new_primary}'")
        pipe.set(drift_key, new_primary)

        pipe.expire(clicks_key, 604800)
        pipe.expire(scores_key, 604800)
        pipe.expire(drift_key, 604800)
        await pipe.execute()

        async with pg_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO user_interactions (user_id, item_id, category, interaction_type, created_at)
                VALUES ($1, $2, $3, $4, $5);
                """,
                event.user_id, event.item_id, event.category, event.interaction_type, datetime.now(timezone.utc)
            )
    except Exception as e:
        print(f"[ERROR] Background pipeline failed: {e}")

# 5. Ingestion Endpoint
@app.post("/events", status_code=202)
async def ingest_event(event: UserEvent, background_tasks: BackgroundTasks, request: Request):
    background_tasks.add_task(
        process_event_background,
        event,
        request.app.state.pg_pool,
        request.app.state.redis
    )
    return {"status": "accepted", "user_id": event.user_id}

# 6. Online Feature Vector Inspection
@app.get("/features/{user_id}")
async def get_features(user_id: int, request: Request):
    r: aioredis.Redis = request.app.state.redis
    clicks = await r.lrange(f"user:{user_id}:recent_clicks", 0, 4)
    scores = await r.hgetall(f"user:{user_id}:category_scores")
    primary = await r.get(f"user:{user_id}:primary_category")
    
    return {
        "user_id": user_id,
        "recent_clicks": clicks,
        "category_affinity_scores": {k: float(v) for k, v in scores.items()},
        "primary_category": primary
    }

# 7. Real-Time Inference: Candidate Scoring with Vector Math
@app.get("/recommend/{user_id}")
async def recommend_items(user_id: int, request: Request, top_k: int = Query(default=3, ge=1, le=10)):
    r: aioredis.Redis = request.app.state.redis
    
    # Fast multi-key fetch from RAM
    scores = await r.hgetall(f"user:{user_id}:category_scores")
    recent_clicks = await r.lrange(f"user:{user_id}:recent_clicks", 0, 4)
    recent_item_ids = {int(x) for x in recent_clicks}

    # Construct the user interest vector
    user_vec = np.zeros(len(ORDERED_CATEGORIES), dtype=np.float32)
    for cat, val in scores.items():
        if cat in CAT_TO_INDEX:
            user_vec[CAT_TO_INDEX[cat]] = float(val)

    user_norm = np.linalg.norm(user_vec)
    
    # Cold-start fallback if user has no clicks yet
    if user_norm == 0.0:
        return {
            "user_id": user_id,
            "strategy": "cold_start_fallback",
            "recommendations": ITEM_CATALOG[:top_k]
        }

    # Vectorized Cosine Similarity: (ITEM_VECTORS @ user_vec) / (norms)
    user_unit_vec = user_vec / user_norm
    item_norms = np.linalg.norm(ITEM_VECTORS, axis=1, keepdims=True)
    item_norms[item_norms == 0] = 1e-9
    item_unit_vectors = ITEM_VECTORS / item_norms

    # Dot product gives exact cosine similarity scores
    similarity_scores = np.dot(item_unit_vectors, user_unit_vec)

    # Rank and filter items the user already interacted with
    scored_items = []
    for idx, score in enumerate(similarity_scores):
        item = ITEM_CATALOG[idx]
        is_recent = item["item_id"] in recent_item_ids
        
        # Penalize items already clicked recently so recommendations stay fresh
        final_score = float(score * 0.2 if is_recent else score)
        
        scored_items.append({
            "item_id": item["item_id"],
            "name": item["name"],
            "category": item["category"],
            "similarity_score": round(final_score, 4),
            "recently_viewed": is_recent
        })

    # Sort descending by similarity
    ranked = sorted(scored_items, key=lambda x: x["similarity_score"], reverse=True)

    return {
        "user_id": user_id,
        "strategy": "real_time_vector_similarity",
        "top_recommendations": ranked[:top_k]
    }

@app.get("/health")
async def health():
    return {"status": "healthy"}