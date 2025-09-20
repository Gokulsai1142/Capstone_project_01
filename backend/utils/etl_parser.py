import yaml, json
from datetime import datetime
import re

def determine_action(step: str):
    step = step.lower()
    if any(x in step for x in ["extract", "read", "import", "fetch", "get", "source"]): return "extract"
    if any(x in step for x in ["transform", "clean", "calculate", "process", "filter", "aggregate", "join", "enrich"]): return "transform"
    if any(x in step for x in ["load", "write", "export", "save", "store", "output"]): return "load"
    return "unknown"

def generate_config(prompt: str, format: str = "json"):
    # Try to extract a name from the prompt, otherwise use a default
    name_match = re.match(r"workflow (.*?) (?:then|,|$)", prompt, re.IGNORECASE)
    workflow_name = name_match.group(1).strip() if name_match else "Generated Workflow"

    # Split steps by various delimiters and action words
    steps_raw = re.split(r"(?:then|,|and|next|after|followed by|to)", prompt, flags=re.IGNORECASE)
    steps = [s.strip() for s in steps_raw if s.strip()]

    data = {
        "workflow": {
            "name": workflow_name,
            "description": prompt,
            "steps": [
                {"id": i+1, "description": step, "action": determine_action(step)}
                for i, step in enumerate(steps)
            ],
            "created": datetime.utcnow().isoformat(),
            "version": "1.0"
        }
    }
    return json.dumps(data, indent=2) if format == "json" else yaml.dump(data)

def generate_dag(prompt: str):
    # Split steps by various delimiters and action words
    steps_raw = re.split(r"(?:then|,|and|next|after|followed by|to)", prompt, flags=re.IGNORECASE)
    steps = [s.strip() for s in steps_raw if s.strip()]

    diagram = "graph TD\n    Start-->Step1;\n"
    for i, step in enumerate(steps):
        diagram += f"    Step{i+1}[{step}];\n"
        if i < len(steps)-1:
            diagram += f"    Step{i+1}-->Step{i+2};\n"
        else:
            diagram += f"    Step{i+1}-->Finish;\n"
    diagram += "    Finish[Finish];"
    return diagram

