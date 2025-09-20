import openai, os

openai.api_key = os.getenv("OPENAI_API_KEY")

def get_smart_fix(error_message: str) -> str:
    try:
        response = openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are an ETL assistant."},
                {"role": "user", "content": f"Fix suggestion for error: {error_message}"}
            ],
            max_tokens=100
        )
        return response.choices[0].message["content"].strip()
    except Exception as e:
        return f"Failed to generate fix: {e}"

