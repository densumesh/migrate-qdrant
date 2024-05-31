import psycopg2
import redis
import os
import uuid

# Connect to the PostgreSQL database
# Get the PostgreSQL connection details from environment variables
conn = psycopg2.connect(
    dbname="trievedb",
    user="foo",
    password="foobarbaz",
    host="trievedb2.cg2fv5r4b3tw.us-east-2.rds.amazonaws.com",
)

# Connect to the Redis server
r = redis.Redis(
    host="oregon-redis.render.com",
    port=6379,
    username="red-cpd3jnu3e1ms73a5q1sg",
    password="eR6waJz6fKSpQNpf55jpV4mJYhNWJBkk",
    ssl=True,
    retry_on_timeout=True,
)


def create_dataset_queue():
    curr = conn.cursor()

    lastBusiness_id = uuid.UUID(int=0)

    while True:
        curr.execute(
            f"SELECT qdrant_point_id FROM chunk_metadata WHERE id > (%s)::uuid AND qdrant_point_id IS NOT NULL ORDER BY id LIMIT 1000",
            (str(lastBusiness_id),),
        )

        # Fetch the first 10000 rows from the result set
        rows = curr.fetchall()

        if not rows:
            break

        # Add the rows to the Redis queue
        for row in rows:
            print(row[0])
            r.rpush("qdrant_ids_to_migrate", str(row[0]))

        lastRecord = rows[-1]
        lastBusiness_id = lastRecord[0]

    r.close()
    curr.close()
    conn.close()


create_dataset_queue()
