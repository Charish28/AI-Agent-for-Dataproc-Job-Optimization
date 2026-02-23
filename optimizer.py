
from typing import Dict, Tuple

from rules import suggest_from_heuristics

from cost_guard import enforce_guardrails

from llm import analyze_with_llm
 
def build_optimized_confs(*, project_id, region, logs, metrics, current_confs, cfg):

    """

    Orchestrates the optimization logic by:

    1. Running heuristic rules (deterministic).

    2. Calling the LLM for deep log analysis.

    3. Merging results and enforcing safety guardrails.

    """

    allow_keys = cfg["guardrails"]["allow_conf_keys"]

    # 1. Get recommendations from Heuristics (rules.py)

    h_recs, h_reason = suggest_from_heuristics(

        metrics, 

        current_confs, 

        cfg["guardrails"], 

        cfg.get("policies", {})

    )

    final_recs = dict(h_recs)

    rationales = [f"Heuristics: {h_reason}"]

    # 2. Get recommendations from LLM (llm.py)

    if cfg["llm"]["enabled"]:

        try:

            l_recs, l_reason, risk = analyze_with_llm(

                project_id=project_id, 

                region=region, 

                model_name=cfg["llm"]["model"],

                logs_sample="\n".join(logs[-500:]), 

                metrics=metrics,

                allow_conf_keys=allow_keys, 

                temperature=cfg["llm"]["temperature"],

                max_output_tokens=cfg["llm"]["max_output_tokens"]

            )

            final_recs.update(l_recs)

            rationales.append(f"LLM ({risk} risk): {l_reason}")

        except Exception as e:

            rationales.append(f"LLM failed: {str(e)}")

    # 3. Final safety check (cost_guard.py)

    final_recs, notes = enforce_guardrails(final_recs, cfg["guardrails"], current_confs)

    if notes:

        rationales.append(f"Guardrails: {notes}")

    return final_recs, " | ".join(rationales)

