import os
from typing import Dict, Any

class SimpleAggregator:
    def __init__(self):
        self.aggregated_data = {}

    def aggregate(self, event_type: str, metrics: Dict[str, Any]):
        if event_type not in self.aggregated_data:
            self.aggregated_data[event_type] = []
        self.aggregated_data[event_type].append(metrics)

    def get_summary(self, event_type: str) -> Dict[str, Any]:
        if event_type not in self.aggregated_data:
            return {}
        
        events = self.aggregated_data[event_type]
        if not events:
            return {}

        # Simple average for numeric metrics
        summary = {}
        for key in events[0].keys():
            values = [e[key] for e in events if key in e and isinstance(e[key], (int, float))]
            if values:
                summary[key] = sum(values) / len(values)
        
        return summary

if __name__ == "__main__":
    agg = SimpleAggregator()
    agg.aggregate("host.metrics", {"cpu_usage": 10, "mem_usage": 20})
    agg.aggregate("host.metrics", {"cpu_usage": 20, "mem_usage": 40})
    print(f"Summary: {agg.get_summary('host.metrics')}")
