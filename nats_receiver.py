import asyncio
import json
import re
import time
from typing import Dict, Any

from loom.nats import (
    ConsumerConfig,
    NatsClient,
    NatsClientOptions,
    RegularStream,
)
from loom.nats.types import JetStreamMessage

# Import your Pulse singleton
from _shared.pulse import pulse
from _shared.metrics import PersonDetectionMetrics

print("🚀 SCRIPT STARTED")

STREAM_NAME = "vision"
VISION_SUBJECT = "vision.>"
CONSUMER_NAME = "vision-debug-consumer14"
ALERT_MAP_FILE = "alerts_mapping.json"



class ParsedResult:
    def __init__(self, score: float, fields: Dict[str, Any]):
        self.score = score
        self.fields = fields



def parse_search_result(sr_text: str) -> ParsedResult:
    # Extract score
    score = 0.0
    score_match = re.search(r"score=([\d.]+)", sr_text)
    if score_match:
        score = float(score_match.group(1))

    # Extract fields dict
    fields_match = re.search(r"fields=({.*})", sr_text)
    if not fields_match:
        raise ValueError("could not find fields dict")

    fields_str = fields_match.group(1)

    # Convert Python-like dict → JSON
    fields_str = fields_str.replace("'", '"')

    try:
        fields = json.loads(fields_str)
    except Exception as e:
        raise ValueError(f"failed to parse fields: {e}")

    return ParsedResult(score=score, fields=fields)


# --------------------------------------------------------------------
# Load alert map
# --------------------------------------------------------------------
def load_alert_map(path: str) -> Dict[str, str]:
    with open(path, "r") as f:
        return json.load(f)

CONFIDENCE_THRESHOLD = 0.6

def print_message(msg: JetStreamMessage, alert_map: Dict[str, str]):
    start = time.time()
    
    # Initialize metrics
    metrics = PersonDetectionMetrics()

    try:
        outer = json.loads(msg.data.decode())
        if not outer:
            raise ValueError("empty outer JSON")
    except Exception as e:
        pulse.logger.error(
            "Could not parse outer JSON",
            {
                "error": str(e),
                "payload": msg.data.decode(errors="ignore"),
            },
        )
        msg.ack()
        return

    sr_text = outer[0]

    try:
        parsed = parse_search_result(sr_text)
        if parsed.score < CONFIDENCE_THRESHOLD:
            pulse.logger.debug(
                "Detection ignored due to low confidence",
                {
                    "score": parsed.score,
                    "threshold": CONFIDENCE_THRESHOLD,
                },
            )
            print(parsed.score)
            print(msg)
        

            msg.ack()
            return
    except Exception as e:
        pulse.logger.error(
            "Parse error",
            {"error": str(e), "payload": msg.data.decode(errors="ignore")},
        )
        msg.ack()
        return

    fields = parsed.fields
    person_name = str(fields.get("name", "")).lower()
    camera_id = fields.get("camera_id", "unknown")
    
    # Update metrics
    metrics = PersonDetectionMetrics(
        detections_total=1,  # Increment by 1 for this detection
        detection_time=time.time(),  # Current timestamp
    )
    
    # Record metrics with labels
    labels = {
        "camera_id": camera_id,
        "person_name": person_name or "unknown",
    }
    
    pulse.metrics.record(metrics, labels=labels)

    if person_name:
        pulse.logger.info(
            "Person detection received",
            {
                "camera_id": camera_id,
                "person_name": person_name,
                "track_id": fields.get("track_id"),
                "score": parsed.score,
                "processing_ms": round((time.time() - start) * 1000, 2),
            },
        )

        # Only alert for known persons
        if person_name in alert_map:
            slack_id = alert_map[person_name]

            pulse.logger.info(
                "Alert required for person",
                {
                    "person_name": person_name,
                    "slack_id": slack_id,
                    "camera_id": fields.get("camera_id"),
                    "track_id": fields.get("track_id"),
                },
            )
        else:
            pulse.logger.debug(
                "No Slack alert for person",
                {"person_name": person_name},
            )

    else:
        pulse.logger.debug("No person name detected in message")

    # --- Keep your pretty print for visibility ---
    print("Parsed SearchResult:")
    print(f"  score      : {parsed.score}")
    print(f"  timestamp  : {fields.get('timestamp')}")
    print(f"  camera_id  : {fields.get('camera_id')}")
    print(f"  name       : {fields.get('name')}")
    print(f"  id         : {fields.get('id')}")
    print(f"  bbox       : {fields.get('bbox')}")
    print(f"  label      : {fields.get('label')}")
    print(f"  track_id   : {fields.get('track_id')}")
    print(f"  processing_time: {(time.time() - start) * 1000:.2f}ms")
    print("-" * 80)

    msg.ack()

    pulse.logger.debug(
        "Message processed",
        {"processing_ms": round((time.time() - start) * 1000, 2)},
    )



async def main():
    client = None

    try:
        alert_map = load_alert_map(ALERT_MAP_FILE)
        print("Loaded alert map:", alert_map)

        pulse.logger.info(
            "Vision NATS listener starting",
            {"stream": STREAM_NAME, "subject": VISION_SUBJECT},
        )

        pulse.logger.info("Connecting to Loom NATS...")
        client = NatsClient(
            NatsClientOptions(url="nats://192.168.0.178:4222")
        )
        pulse.logger.info("Connected to NATS")

        # Bind to existing stream
        stream = client.jetstream.stream.create_stream(
            RegularStream(STREAM_NAME, [VISION_SUBJECT])
        )
        print("Using stream:", stream.stream.config.name)

        # Create durable pull consumer
        consumer = stream.consumer.create_consumer(
            ConsumerConfig(
                name=CONSUMER_NAME,
                filter_subject=VISION_SUBJECT,
                deliver_policy="last",
            )
        )

        print("Listening on:", VISION_SUBJECT)
        print("-" * 80)

        # ---- Read ONE existing message first (5s timeout) ----
        pull = consumer.subscribe_pull(timeout=5.0)

        try:
            msg = next(pull)
            print_message(msg, alert_map)
        except StopIteration:
            print("No existing message in stream.")

        pull.close()

        # ---- Continuous listening ----
        pull = consumer.subscribe_pull(timeout=60.0)

        while True:
            try:
                msg = next(pull)
                print_message(msg, alert_map)
            except StopIteration:
                # keep alive every 60s
                pulse.logger.debug("NATS keepalive - no new messages")

    except Exception as e:
        pulse.logger.error("Fatal error in listener", {"error": str(e)})

    finally:
        if client:
            try:
                client.close()
                pulse.logger.info("NATS client closed.")
            except Exception:
                pass


if __name__ == "__main__":
    asyncio.run(main())
