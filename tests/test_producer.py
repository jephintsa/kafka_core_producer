import pytest
from common.producer import build_event

def test_build_event():
    metrics = {"cpu": 50}
    tags = {"env": "test"}
    event = build_event(
        event_type="test.event",
        source="test_source",
        metrics=metrics,
        tags=tags,
        host="test-host"
    )
    
    assert event["event_type"] == "test.event"
    assert event["metrics"]["cpu"] == 50
    assert event["tags"]["env"] == "test"
    assert event["host"] == "test-host"
    assert "timestamp" in event
    assert event["timestamp"].endswith("Z")

def test_build_event_no_optional_args():
    metrics = {"cpu": 50}
    event = build_event(
        event_type="test.event",
        source="test_source",
        metrics=metrics
    )
    
    assert event["event_type"] == "test.event"
    assert event["source"] == "test_source"
    assert event["metrics"]["cpu"] == 50
    assert "tags" in event
    assert event["tags"] == {}
