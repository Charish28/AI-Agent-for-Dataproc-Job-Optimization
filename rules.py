
from typing import Dict, Tuple
 
def suggest_from_heuristics(metrics: Dict, current_confs: Dict, guardrails: Dict, policies: Dict) -> Tuple[Dict[str, str], str]:

    recs = {}

    rationale = []

    # 1. Shuffle partitions: Target ~128MB per partition

    # Defaulting to 10GB if metrics aren't found for the dry run

    shuffle_bytes = metrics.get("shuffle_bytes", 10 * 1024**3) 

    target_parts = max(64, min(4096, int(shuffle_bytes / (128 * 1024**2))))

    recs["spark.sql.shuffle.partitions"] = str(target_parts)

    rationale.append(f"Set partitions to {target_parts} based on ~10GB shuffle size.")
 
    # 2. GC/Memory pressure: If GC time > 15%, increase memory

    if metrics.get("gc_time_pct", 0) > 15:

        cur_mem = int(current_confs.get("spark.executor.memory", "8g").replace("g", ""))

        new_mem = min(cur_mem + 2, guardrails.get("max_executor_memory_gb", 32))

        recs["spark.executor.memory"] = f"{new_mem}g"

        rationale.append(f"High GC pressure; increased memory to {new_mem}g.")
 
    return recs, "; ".join(rationale)

