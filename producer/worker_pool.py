import asyncio
import json

from aiokafka import AIOKafkaProducer
import asyncpg

from producer.consts import DB_DSN, BATCH_SIZE, POLL_INTERVAL, SQL_STATEMENT, KAFKA_TOPIC, KAFKA_BROKER, NUM_WORKERS


class Worker:
    def __init__(self, worker_id: int, producer: AIOKafkaProducer):
        self.worker_id = worker_id
        self.producer = producer
        self.last_id = 0

    async def execute(self):
        conn = await asyncpg.connect(DB_DSN)
        try:
            while True:
                rows = await conn.fetch(
                    SQL_STATEMENT,
                    self.last_id,
                    BATCH_SIZE * NUM_WORKERS
                )

                # фильтруем по воркеру
                rows = [r for r in rows if r["id"] % NUM_WORKERS == self.worker_id]

                if not rows:
                    await asyncio.sleep(POLL_INTERVAL)
                    continue

                for row in rows:
                    self.last_id = row["id"]
                    event = {
                        "id": row["id"],
                        "ad_id": row["ad_id"],
                        "ad_event": row["ad_event"],
                        "user_id": row["user_id"],
                    }
                    await self.producer.send_and_wait(
                        KAFKA_TOPIC, json.dumps(event).encode("utf-8")
                    )

                print(f"Worker {self.worker_id}: sent batch of {len(rows)} events")

        finally:
            await conn.close()


class WorkerPool:
    def __init__(self, kafka_broker: str):
        self.kafka_broker = kafka_broker

    async def start(self, workers_num: int):
        producer = AIOKafkaProducer(
            bootstrap_servers=self.kafka_broker,
            acks=1,
            enable_idempotence=False,
        )
        await producer.start()
        try:
            tasks = [Worker(i, producer).execute() for i in range(workers_num)]
            await asyncio.gather(*tasks)
        finally:
            await producer.stop()


if __name__ == "__main__":
    pool = WorkerPool(KAFKA_BROKER)
    asyncio.run(pool.start(NUM_WORKERS))
