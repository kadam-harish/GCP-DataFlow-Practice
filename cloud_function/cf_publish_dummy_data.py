import json
import random
from google.cloud import pubsub_v1

publisher = pubsub_v1.PublisherClient()
TOPIC_PATH = "projects/fast-level-up-487010/topics/orders-topic"

def publish_dummy(request):

    cities = ["Mumbai", "Delhi", "Pune", "Bangalore", "Chennai"]

    message = {
        "order_id": str(random.randint(1000, 9999)),
        "amount": round(random.uniform(50, 500), 2),
        "city": random.choice(cities)
    }

    future = publisher.publish(
        TOPIC_PATH,
        json.dumps(message).encode("utf-8")
    )

    message_id = future.result()

    return {
        "status": "published",
        "message_id": message_id,
        "payload": message
    }
