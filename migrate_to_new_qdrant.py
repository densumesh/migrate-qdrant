import qdrant_client.conversions
import qdrant_client.conversions.common_types
import redis
import qdrant_client
import qdrant_client.http.models as models
import dotenv
import os

dotenv.load_dotenv()

# Connect to the Redis server
r = redis.Redis(
    host=os.getenv("REDIS_HOST"),
    port=6379,
    username=os.getenv("REDIS_USERNAME"),
    password=os.getenv("REDIS_PASSWORD"),
    ssl=True,
    retry_on_timeout=True,
)

# Connect to the Qdrant server
old_qdrant = qdrant_client.QdrantClient(
    host=os.getenv("OLD_QDRANT_HOST"),
    api_key=os.getenv("OLD_QDRANT_API_KEY"),
    prefer_grpc=True,
    https=False,
)

new_qdrant = qdrant_client.QdrantClient(
    host=os.getenv("NEW_QDRANT_HOST"),
    api_key=os.getenv("NEW_QDRANT_API_KEY"),
)


while True:
    # Fetch the first 10000 rows from the Redis queue
    rows = r.lrange("qdrant_ids_to_migrate", 0, 1000)

    if not rows:
        break

    # Migrate the rows from the old Qdrant server to the new Qdrant server
    chunk_size = 10
    for i in range(0, len(rows) - 10, chunk_size):
        chunk = rows[i : i + chunk_size]
        old_point = old_qdrant.scroll(
            collection_name=os.getenv("OLD_QDRANT_COLLECTION_NAME"),
            scroll_filter=models.Filter(
                must=[
                    models.HasIdCondition(has_id=[row.decode("utf-8")]) for row in chunk
                ]
            ),
            limit=10,
            with_payload=True,
            with_vectors=True,
        )

        points = [
            models.PointStruct(
                id=inner_point[0].id,
                vector=inner_point[0].vector,  # type: ignore
                payload=inner_point[0].payload,
            )
            for inner_point in old_point
        ]

        new_qdrant.upsert(
            collection_name=[
                key for key in old_point[0][0].vector.keys() if key != "sparse_vectors"
            ][0],
            points=points,
        )

    # Remove the rows from the Redis queue
    r.ltrim("qdrant_ids_to_migrate", len(rows), -1)
