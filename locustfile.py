import random
from locust import HttpUser, task, between

CATEGORIES = ["electronics", "apparel", "home", "books", "gaming", "fitness"]

class FeatureStoreLoadTester(HttpUser):
    # Simulated user wait time between actions: 50ms to 200ms
    wait_time = between(0.05, 0.2)

    # 70% of traffic simulates user browsing/clicking (writes)
    @task(7)
    def post_user_event(self):
        user_id = random.randint(1, 100)
        payload = {
            "user_id": user_id,
            "item_id": random.randint(100, 999),
            "category": random.choice(CATEGORIES),
            "interaction_type": "click"
        }
        self.client.post("/events", json=payload, name="/events [POST Ingestion]")

    # 30% of traffic simulates real-time feed rendering / recommendations (reads + vector math)
    @task(3)
    def get_user_recommendations(self):
        user_id = random.randint(1, 100)
        self.client.get(f"/recommend/{user_id}?top_k=3", name="/recommend [GET Inference]")
