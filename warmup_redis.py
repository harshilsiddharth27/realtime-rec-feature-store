import asyncio
import redis.asyncio as aioredis
import asyncpg

DB_CONFIG = {
    "user": "feature_user",
    "password": "feature_password",
    "database": "feature_warehouse",
    "host": "localhost",
    "port": 5432
}

REDIS_URL = "redis://localhost:6379/0"

async def sync_offline_to_online():
    print("[*] Connecting to PostgreSQL and Redis...")
    pg_conn = await asyncpg.connect(**DB_CONFIG)
    r = aioredis.from_url(REDIS_URL, decode_responses=True)

    try:
        # Flush Redis DB 0 to start fresh
        await r.flushdb()
        print("[+] Redis cleared for fresh warmup.")

        # Query the top 100 users and their 5 most recent interactions from Postgres
        print("[*] Querying latest user interactions from PostgreSQL...")
        recent_interactions = await pg_conn.fetch("""
            WITH ranked_events AS (
                SELECT 
                    user_id, 
                    item_id, 
                    category,
                    ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY created_at DESC) as rank
                FROM user_interactions
                WHERE interaction_type = 'click'
            )
            SELECT user_id, item_id, category 
            FROM ranked_events 
            WHERE rank <= 5
            ORDER BY user_id, rank ASC;
        """)

        # Use a Redis pipeline to batch write operations (avoids round-trip latency)
        pipe = r.pipeline()
        synced_users = set()

        for record in recent_interactions:
            uid = record["user_id"]
            item_id = record["item_id"]
            cat = record["category"]
            synced_users.add(uid)

            # 1. Push to recent clicks list
            pipe.rpush(f"user:{uid}:recent_clicks", item_id)
            # 2. Increment category preference counter in a Hash
            pipe.hincrby(f"user:{uid}:category_clicks", cat, 1)
            # 3. Set a 7-day TTL so stale cached data auto-expires
            pipe.expire(f"user:{uid}:recent_clicks", 604800)
            pipe.expire(f"user:{uid}:category_clicks", 604800)

        print(f"[*] Executing pipeline with {len(pipe)} queued commands...")
        await pipe.execute()
        print(f"[+] Successfully warmed up Redis features for {len(synced_users)} users.")

        # Verify: Fetch features for User 1
        test_user = 1
        user_clicks = await r.lrange(f"user:{test_user}:recent_clicks", 0, -1)
        user_categories = await r.hgetall(f"user:{test_user}:category_clicks")

        print(f"\n--- Verification Sample: User {test_user} ---")
        print(f"  Recent Clicks (List)   : {user_clicks}")
        print(f"  Category Counts (Hash) : {user_categories}")

    finally:
        await pg_conn.close()
        await r.aclose()
        print("\n[*] Connections closed cleanly.")

if __name__ == "__main__":
    asyncio.run(sync_offline_to_online())
