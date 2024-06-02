import threading
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
    port=80,
    https=False,
)


while True:
    # Fetch the first 10000 rows from the Redis queue
    rows = r.spop("qdrant_ids_to_migrate", count=1000)

    if not rows:
        break

    # Migrate the rows from the old Qdrant server to the new Qdrant server
    chunk_size = 50
    chunks = [rows[i : i + chunk_size] for i in range(0, len(rows), chunk_size)]

    def process_rows(chunk):
        try:
            old_point = old_qdrant.scroll(
                collection_name=os.getenv("OLD_QDRANT_COLLECTION_NAME"),
                scroll_filter=models.Filter(
                    should=[models.HasIdCondition(has_id=[row]) for row in chunk]
                ),
                limit=chunk_size,
                with_payload=True,
                with_vectors=True,
            )

            points = [
                models.PointStruct(
                    id=inner_point.id,
                    vector=inner_point.vector,  # type: ignore
                    payload=inner_point.payload,
                )
                for inner_point in old_point[0]
            ]

            for point in points:
                new_qdrant.upsert(
                    collection_name=[
                        key for key in point.vector.keys() if key != "sparse_vectors"  # type: ignore
                    ][0],
                    points=[point],
                )

                r.sadd("migrated_qdrant_ids", str(point.id))
        except Exception as e:
            print(e)
            r.sadd("failed_qdrant_ids", *[row for row in chunk])

    threads = []
    for chunk in chunks:
        t = threading.Thread(target=process_rows, args=(chunk,))
        t.start()
        threads.append(t)

    # Wait for all threads to finish
    for t in threads:
        t.join()

    # Remove the rows from the Redis queue
