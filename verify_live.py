import asyncio
import redis.asyncio as aioredis
import asyncpg

REDIS_URL = "redis://localhost:6379/0"
POSTGRES_DSN = "postgresql://feature_user:feature_password@localhost:5432/feature_warehouse"

async def inspect_user(user_id: int = 1):
    # 1. Inspect Redis Feature Store
    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    clicks = await r.lrange(f"user:{user_id}:recent_clicks", 0, 4)
    categories = await r.hgetall(f"user:{user_id}:category_clicks")
    await r.aclose()

    print(f"\n--- [REDIS ONLINE STORE] User {user_id} ---")
    print(f"Recent Clicks (Window of 5) : {clicks}")
    print(f"Category Affinity (Hash)   : {categories}")

    # 2. Inspect PostgreSQL Offline Warehouse
    conn = await asyncpg.connect(POSTGRES_DSN)
    latest_event = await conn.fetchrow(
        """
        SELECT item_id, category, interaction_type, created_at 
        FROM user_interactions 
        WHERE user_id = $1 
        ORDER BY created_at DESC 
        LIMIT 1;
        """,
        user_id
    )
    await conn.close()

    print(f"\n--- [POSTGRES OFFLINE STORE] User {user_id} ---")
    if latest_event:
        print(f"Latest Persisted Event      : item={latest_event['item_id']}, "
              f"category={latest_event['category']}, type={latest_event['interaction_type']}, "
              f"time={latest_event['created_at']}")
    else:
        print("No interactions found in Postgres.")

if __name__ == "__main__":
    asyncio.run(inspect_user(1))