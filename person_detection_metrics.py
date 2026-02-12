#!/usr/bin/env python3
"""
Person Detection Metrics Test Generator
---------------------------------------

This version now:
- Emits +1 per detection (correct Prometheus counter pattern)
- Uses consistent labels with your NATS listener
- Generates realistic streams that match your alerts
- Will properly trigger your PersonDetection alert rule
"""

import pulse
from pulse import (
    Pulse,
    ServiceOptions,
    PulseOptions,
    Environment,
    TelemetryOptions,
    OTLPOptions,
    FoxgloveOptions,
    MetricsBaseModel,
)
import time
import random


# ------------------------------------------------------------------
# PERSON DETECTION METRIC MODEL (same as your real service)
# ------------------------------------------------------------------
class PersonDetectionMetrics(MetricsBaseModel, prefix="vision.person"):
    """Person detection metrics"""

    detections_total: int = pulse.Counter(
        description="Total person detections"
    )
    detection_score: float = pulse.Gauge(
        description="Person detection confidence score"
    )


def main():
    # ------------------------------------------------------------------
    # SERVICE CONFIG
    # ------------------------------------------------------------------
    service_opts = ServiceOptions(
        name="vision-test-generator",
        version="1.0.0",
        description="Synthetic person detection metrics for testing alerts",
        environment=Environment.DEVELOPMENT,
    )

    pulse_opts = PulseOptions(
        telemetry=TelemetryOptions(
            otlp=OTLPOptions(
                enabled=True,
                endpoint="localhost"
            ),
        ),
        foxglove=FoxgloveOptions(
            enabled=True,
            mcap_path="metrics-data.mcap",
        ),
    )

    # ------------------------------------------------------------------
    # RUN WITH PULSE CONTEXT MANAGER
    # ------------------------------------------------------------------
    with Pulse(service_opts, pulse_opts) as p:
        p.logger.info("Synthetic Person Detection Metrics Started")
        p.logger.info("Sending OTLP metrics to localhost")

        # People you are testing
        test_persons = [
            ("akash", "cam_01", "59"),
            ("vignesh", "cam_01", "72"),
            ("harsha", "cam_01", "59"),
            ("unknown", "cam_01", "91"),
            ("kailash", "cam_01", "72"),
        ]

        total_detections = 0

        # Simulate 12 batches
        for i in range(12):
            p.logger.info("Processing detection batch", {"iteration": i})

            for person_name, camera_id, track_id in test_persons:

                # --- CORRECT COUNTER PATTERN ---
                total_detections += 1

                person_metrics = PersonDetectionMetrics(
                    detections_total=1,  # ALWAYS +1 per detection ✅
                    detection_score=random.uniform(0.3, 0.9),
                )

                labels = {
                    "camera_id": camera_id,
                    "person_name": person_name,
                    "track_id": track_id,
                }

                p.metrics.record(person_metrics, labels=labels)

                p.logger.info(
                    "Person detection recorded",
                    {
                        "detection_score": person_metrics.detection_score,
                        "global_total": total_detections,
                        "camera_id": camera_id,
                        "person_name": person_name,
                        "track_id": track_id,
                    },
                )

                time.sleep(0.3)

            time.sleep(0.5)

        p.logger.info("Synthetic test completed!")
        p.logger.info("Check Prometheus: http://localhost:9090/alerts")
        p.logger.info("Check Grafana: http://localhost:3000")

        p.close()
        print(service_opts)
        print(pulse_opts)


if __name__ == "__main__":
    main()
