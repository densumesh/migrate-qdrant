import psycopg2
import redis
import os
import uuid
import threading

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
    host="hackernews-queue.tnzwmz.0001.use2.cache.amazonaws.com",
    port=6379,
    retry_on_timeout=True,
)


def create_dataset_queue():
    curr = conn.cursor()

    lastBusiness_id = uuid.UUID(int=0)
    currentBusiness_id = uuid.UUID(int=0)

    while True:
        try:
            curr.execute(
                f"SELECT id, qdrant_point_id FROM chunk_metadata WHERE id > (%s)::uuid AND qdrant_point_id IS NOT NULL ORDER BY id LIMIT 10000",
                (str(lastBusiness_id),),
            )
            print(f"Fetching rows after {lastBusiness_id}...")

            # Fetch the first 10000 rows from the result set
            rows = curr.fetchall()

            if not rows:
                break

            # Add the rows to the Redis queue
            def process_rows(rows):
                for row in rows:
                    currentBusiness_id = row[0]
                    if r.sismember("qdrant_ids_to_migrate", str(row[1])) == 0:
                        print(f"Adding {row[1]} to the queue")
                        r.sadd("qdrant_ids_to_migrate", str(row[1]))

            # Chunk the rows into 10 arrays
            chunk_size = len(rows) // 100
            chunks = [rows[i : i + chunk_size] for i in range(0, len(rows), chunk_size)]

            threads = []
            for chunk in chunks:
                t = threading.Thread(target=process_rows, args=(chunk,))
                t.start()
                threads.append(t)

            # Wait for all threads to finish
            for t in threads:
                t.join()

            lastRecord = rows[-1]
            lastBusiness_id = lastRecord[0]
        except Exception as e:
            print(e)
            lastBusiness_id = currentBusiness_id

    r.close()
    curr.close()
    conn.close()


create_dataset_queue()
