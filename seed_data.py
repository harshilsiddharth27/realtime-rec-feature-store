import asyncio
import random
from datetime import datetime, timedelta
import asyncpg

DB_CONFIG = {
    "user": "feature_user",
    "password": "feature_password",
    "database": "feature_warehouse",
    "host": "localhost",
    "port": 5432
}

CATEGORIES = ["electronics", "apparel", "home", "books", "gaming", "fitness"]
INTERACTIONS = ["view", "click", "click", "click", "purchase"]  # clicks are more frequent
COUNTRIES = ["IN", "US", "UK", "DE", "JP", "CA"]

async def seed_database():
    print("[*] Connecting to PostgreSQL...")
    conn = await asyncpg.connect(**DB_CONFIG)
    
    try:
        # Step A: Apply Schema
        print("[*] Reading and executing schema.sql...")
        with open("schema.sql", "r") as f:
            schema_sql = f.read()
        await conn.execute(schema_sql)
        print("[+] Tables and indexes successfully created.")

        # Step B: Check if already seeded
        existing_users = await conn.fetchval("SELECT COUNT(*) FROM users;")
        if existing_users > 0:
            print(f"[!] Database already contains {existing_users} users. Clearing old data...")
            await conn.execute("TRUNCATE TABLE user_interactions, users RESTART IDENTITY CASCADE;")

        # Step C: Generate Synthetic Users
        num_users = 100
        user_records = []
        for i in range(1, num_users + 1):
            user_records.append((
                f"user_{i}",
                random.randint(18, 65),
                random.choice(COUNTRIES)
            ))

        print(f"[*] Bulk inserting {num_users} users using copy_records_to_table...")
        await conn.copy_records_to_table(
            "users",
            records=user_records,
            columns=["username", "age", "country"]
        )
        print("[+] Users inserted.")

        # Step D: Generate Synthetic User Interactions
        num_interactions = 5000
        interaction_records = []
        base_time = datetime.now()

        for _ in range(num_interactions):
            uid = random.randint(1, num_users)
            item_id = random.randint(100, 999)
            cat = random.choice(CATEGORIES)
            itype = random.choice(INTERACTIONS)
            # Spread interactions randomly over the past 30 days
            event_time = base_time - timedelta(
                days=random.randint(0, 30),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )
            interaction_records.append((uid, item_id, cat, itype, event_time))

        print(f"[*] Bulk inserting {num_interactions} interactions...")
        await conn.copy_records_to_table(
            "user_interactions",
            records=interaction_records,
            columns=["user_id", "item_id", "category", "interaction_type", "created_at"]
        )
        print(f"[+] {num_interactions} interactions successfully populated.")

        # Step E: Quick Verification Query
        sample_metric = await conn.fetch("""
            SELECT category, COUNT(*) as count 
            FROM user_interactions 
            GROUP BY category 
            ORDER BY count DESC;
        """)
        print("\n--- Event Distribution by Category ---")
        for row in sample_metric:
            print(f"  {row['category']:<12}: {row['count']} events")

    finally:
        await conn.close()
        print("\n[*] Connection closed cleanly.")

if __name__ == "__main__":
    asyncio.run(seed_database())
