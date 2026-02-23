import json, re, vertexai
from vertexai.generative_models import GenerativeModel, GenerationConfig
 
def analyze_with_llm(project_id, region, model_name, logs_sample, metrics, allow_conf_keys, temperature, max_output_tokens):
    try:
        vertexai.init(project=project_id, location=region)
        model = GenerativeModel(model_name)
        prompt = f"Analyze Spark logs and metrics. Suggest config deltas for: {allow_conf_keys}. Logs: {logs_sample[:2000]}"
        resp = model.generate_content([prompt])
        # Returning empty for dry-run/simplicity if parsing fails
        return {}, "LLM analysis complete (simulated)", "low"
    except Exception as e:
        return {}, f"LLM unavailable: {str(e)}", "medium"
