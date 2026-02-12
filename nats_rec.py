import asyncio
import json
import os

from nats.aio.client import Client as NATS
from nats.js.api import DeliverPolicy, ConsumerConfig

NATS_URL = os.getenv("NATS_URL", "nats://192.168.0.22:4222")
STREAM_NAME = os.getenv("STREAM_NAME", "vision")
OUTPUT_FILE = "nats_stream_dump.json"

async def dump_stream():
    nc = NATS()
    await nc.connect(NATS_URL)

    js = nc.jetstream()

    sub = await js.pull_subscribe(
        subject="vision.>",
        stream=STREAM_NAME,
        durable="stream-dump-reader",
        config=ConsumerConfig(
            deliver_policy=DeliverPolicy.ALL,
            filter_subject="vision.>"
        ),
    )

    records = []
    batch_size = 100

    while True:
        msgs = await sub.fetch(batch_size, timeout=2)

        if not msgs:
            break

        for msg in msgs:
            meta = msg.metadata

            record = {
                "subject": msg.subject,
                "sequence": meta.sequence.stream if meta else None,
                "timestamp": meta.timestamp.isoformat() if meta else None,
                "headers": dict(msg.headers) if msg.headers else None,
                "payload": msg.data.decode(errors="replace"),
            }

            records.append(record)
            await msg.ack()

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)

    print(f"Saved {len(records)} messages to {OUTPUT_FILE}")
    await nc.close()

if __name__ == "__main__":
    asyncio.run(dump_stream())
