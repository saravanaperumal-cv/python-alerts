import asyncio
from nats.aio.client import Client as NATS
from nats.js.api import ConsumerConfig, DeliverPolicy


NATS_URL = "nats://192.168.0.178:4222"
SUBJECT = "vision.face.known"
STREAM = "vision"
DURABLE = "vision-face-known-debug"


async def main():
    nc = NATS()
    await nc.connect(NATS_URL)
    print(f"Connected to {NATS_URL}")

    js = nc.jetstream()

    async def message_handler(msg):
        print("--------------------------------------------------")
        print(f"Subject     : {msg.subject}")
        print(f"Data        : {msg.data.decode(errors='ignore')}")
        print(f"Stream Seq  : {msg.metadata.sequence.stream}")
        print("--------------------------------------------------")

        await msg.ack()

    # IMPORTANT: explicitly define filter_subject + durable
    await js.subscribe(
        subject=SUBJECT,
        stream=STREAM,
        durable=DURABLE,
        cb=message_handler,
        config=ConsumerConfig(
            durable_name=DURABLE,
            filter_subject=SUBJECT,
            deliver_policy=DeliverPolicy.ALL,  # replay existing
            ack_policy="explicit",
        ),
    )

    print(f"Listening on {SUBJECT} (stream={STREAM})...")
    print("Replaying existing + new messages")

    while True:
        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
