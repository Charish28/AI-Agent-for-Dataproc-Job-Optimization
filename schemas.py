from typing import Dict, Any, Optional, List
from dataclasses import dataclass
 
@dataclass
class Telemetry:
    job_id: str
    cluster_name: Optional[str]
    project_id: str
    region: str
    logs: List[str]
    metrics: Dict[str, Any]
 
@dataclass
class OptimizationProposal:
    recommended_confs: Dict[str, str]
    rationale: str
    risk_level: str
 
@dataclass
class SubmissionRequest:
    job_id: Optional[str]
    main_python_file_uri: Optional[str]
    args: List[str]
    properties: Dict[str, str]
    cluster_name: Optional[str]
    service_account: Optional[str]
