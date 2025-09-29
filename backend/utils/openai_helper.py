import os
import json
import re

# Don't initialize OpenAI client at import time
_openai_client = None

def get_openai_client():
    """Lazy initialization of OpenAI client"""
    global _openai_client
    
    if _openai_client is None:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            return None
        
        try:
            import openai
            _openai_client = openai.OpenAI(api_key=api_key)
        except Exception as e:
            print(f"Failed to initialize OpenAI client: {e}")
            return None
    
    return _openai_client

def get_smart_fix(error_message: str) -> str:
    """Get smart fix suggestion using OpenAI"""
    client = get_openai_client()
    
    if not client:
        return "OpenAI not available. Please check your API key configuration."
    
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are an ETL assistant."},
                {"role": "user", "content": f"Fix suggestion for error: {error_message}"}
            ],
            max_tokens=100
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        return f"Failed to generate fix: {e}"

def generate_config_with_ai(prompt: str, format: str = "json") -> str:
    """Use OpenAI to generate ETL configuration from natural language"""
    client = get_openai_client()
    
    if not client:
        print("OpenAI not available, falling back to regex parsing")
        from .etl_parser import generate_config
        return generate_config(prompt, format)
    
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system", 
                    "content": """You are an ETL workflow parser. Convert natural language descriptions 
                    into structured ETL workflow configuration. Return a JSON object with:
                    {
                        "workflow": {
                            "name": "descriptive name",
                            "description": "original prompt",
                            "steps": [
                                {"id": 1, "description": "step description", "action": "extract|transform|load"}
                            ],
                            "created": "ISO timestamp",
                            "version": "1.0"
                        }
                    }
                    Actions should be: extract (read/import data), transform (clean/calculate/filter), load (save/export)"""
                },
                {"role": "user", "content": f"Parse this ETL workflow: {prompt}"}
            ],
            max_tokens=500
        )
        
        ai_response = response.choices[0].message.content.strip()
        
        # Try to parse and validate JSON response
        try:
            parsed = json.loads(ai_response)
            return json.dumps(parsed, indent=2) if format == "json" else ai_response
        except json.JSONDecodeError:
            # Fallback to regex parsing
            print("AI response was not valid JSON, falling back to regex")
            from .etl_parser import generate_config
            return generate_config(prompt, format)
            
    except Exception as e:
        print(f"OpenAI API error: {e}, falling back to regex parsing")
        # Fallback to regex-based parsing
        from .etl_parser import generate_config
        return generate_config(prompt, format)

def get_transformation_suggestions(columns: list, prompt: str) -> str:
    """Get AI suggestions for data transformations"""
    client = get_openai_client()
    
    if not client:
        return "OpenAI not configured. Using rule-based transformations."
    
    try:
        columns_str = ", ".join(columns)
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": "You are a data transformation expert. Suggest pandas operations based on column names and user requests."
                },
                {
                    "role": "user",
                    "content": f"Available columns: {columns_str}\nUser request: {prompt}\nSuggest specific transformations:"
                }
            ],
            max_tokens=200
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        return f"AI suggestion error: {e}"

def is_openai_available() -> bool:
    """Check if OpenAI is available and configured"""
    return get_openai_client() is not None