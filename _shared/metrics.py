import pulse
from pulse import MetricsBaseModel


class PersonDetectionMetrics(MetricsBaseModel, prefix="vision.person"):
    """
    Metrics for NATS person-detection pipeline.
    All metrics will appear in Prometheus as:
    vision_person_*
    """
    
    detections_total: int = pulse.Counter(
        description="Total number of person detections processed"
    )
    
    detection_time: float = pulse.Gauge(
        description="Time of last person detection"
    )

