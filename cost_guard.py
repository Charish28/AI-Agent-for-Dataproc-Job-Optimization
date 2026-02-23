from typing import Dict, Tuple
 
def enforce_guardrails(recs: Dict[str, str], guardrails: Dict, current_confs: Dict) -> Tuple[Dict[str, str], str]:
    out = {}
    notes = []
    allow = set(guardrails.get("allow_conf_keys", []))
 
    for k, v in recs.items():
        if k in allow:
            out[k] = v
        else:
            notes.append(f"Blocked key: {k}")
    return out, "; ".join(notes)
