import datetime as dt
from typing import List, Dict, Any, Optional
from google.cloud import logging_v2
from schemas import Telemetry

def collect_telemetry(project_id, region, job_id=None, cluster_name=None):
    print(f"\n--- My_Agent: Analyzing Cluster {cluster_name} ---")

    logs = []

    try:
        client = logging_v2.Client(project=project_id)
        filter_str = f'resource.labels.cluster_name="{cluster_name}"'
        entries = client.list_entries(filter_=filter_str, max_results=100)

        for e in entries:
            if hasattr(e, "text_payload") and e.text_payload:
                logs.append(e.text_payload)
            elif hasattr(e, "json_payload") and e.json_payload:
                logs.append(str(e.json_payload))

    except Exception as ex:
        print("Logging API failed, using fallback telemetry:", ex)

    # Mock metrics (safe fallback)
    metrics = {
        "shuffle_bytes": 10 * 1024**3,
        "spill_mb": 500,
        "gc_time_pct": 5.0,
        "skew_score": 0.2
    }

    return Telemetry(
        job_id or "na",
        cluster_name,
        project_id,
        region,
        logs,
        metrics
    )
