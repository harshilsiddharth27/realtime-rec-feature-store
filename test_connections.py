import asyncio
import redis.asyncio as aioredis
import asyncpg

async def verify():
    # 1. Test Redis
    try:
        r = aioredis.from_url("redis://localhost:6379/0", decode_responses=True)
        await r.set("healthcheck", "ok")
        val = await r.get("healthcheck")
        print(f"[SUCCESS] Redis connected! Healthcheck value: {val}")
        await r.aclose()
    except Exception as e:
        print(f"[FAIL] Redis connection error: {e}")

    # 2. Test PostgreSQL
    try:
        conn = await asyncpg.connect(
            user="feature_user",
            password="feature_password",
            database="feature_warehouse",
            host="localhost",
            port=5432
        )
        version = await conn.fetchval("SELECT version();")
        print(f"[SUCCESS] PostgreSQL connected! Version: {version[:25]}...")
        await conn.close()
    except Exception as e:
        print(f"[FAIL] PostgreSQL connection error: {e}")

if __name__ == "__main__":
    asyncio.run(verify())
