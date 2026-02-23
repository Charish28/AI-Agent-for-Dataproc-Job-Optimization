
import argparse

import yaml

from typing import Dict

import telemetry

import optimizer

import dataproc_client
 
def load_cfg(path: str) -> Dict:

    with open(path, "r") as f:

        return yaml.safe_load(f)
 
def run():

    parser = argparse.ArgumentParser(description="My_Agent: Self Optimizing Spark Agent")

    parser.add_argument("--config", default="config.yaml")

    parser.add_argument("--project_id", required=False)

    parser.add_argument("--region", required=False)

    parser.add_argument("--cluster_name", required=True)

    parser.add_argument("--main_py", required=True)

    parser.add_argument("--args", nargs="*", default=[])

    parser.add_argument("--dry_run", action="store_true")

    parser.add_argument("--base_conf", nargs="*", default=[])

    args = parser.parse_args()

    cfg = load_cfg(args.config)

    # 1. Collect Telemetry

    print(f"\n--- My_Agent: Analyzing Cluster {args.cluster_name} ---")

    tel = telemetry.collect_telemetry(

        project_id=args.project_id or cfg['project_id'],

        region=args.region or cfg['region'],

        job_id=None,

        cluster_name=args.cluster_name

    )
 
    # 2. Generate Optimizations

    base_props = dict(item.split('=') for item in args.base_conf) if args.base_conf else {}

    optimized_props, rationale = optimizer.build_optimized_confs(

        project_id=cfg['project_id'],

        region=cfg['region'],

        logs=tel.logs,

        metrics=tel.metrics,

        current_confs=base_props,

        cfg=cfg

    )
 
    print(f"\nAI Rationale: {rationale}")

    print(f"Final Configs: {optimized_props}")
 
    # 3. Submit Job

    if args.dry_run:

        print("\n[DRY RUN] Would submit job with optimized configurations.")

    else:

        print("\nSubmitting optimized job to Dataproc...")

        job_id = dataproc_client.submit_cluster_job(

            project_id=cfg['project_id'],

            region=cfg['region'],

            cluster_name=args.cluster_name,

            properties=optimized_props,

            main_py=args.main_py,

            args=args.args

        )

        print(f"Job submitted successfully! Job ID: {job_id}")
 
if __name__ == "__main__":

    run()

