from fastapi import FastAPI, UploadFile, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import pandas as pd
import io, os
from utils.etl_parser import generate_config, generate_dag
from utils.transformer import transform_data
from utils.openai_helper import get_smart_fix

app = FastAPI()

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve frontend
frontend_dir = os.path.join(os.path.dirname(__file__), "..", "frontend")
app.mount("/frontend", StaticFiles(directory=frontend_dir), name="frontend")

@app.get("/")
def root():
    return FileResponse(os.path.join(frontend_dir, "index.html"))

@app.post("/generate-workflow")
async def generate_workflow(
    prompt: str = Form(...),
    target_format: str = Form("json"),
    file: UploadFile = None
):
    df = None
    if file:
        content = await file.read()
        if file.filename.endswith(".csv"):
            df = pd.read_csv(io.BytesIO(content))
        elif file.filename.endswith((".xlsx", ".xls")):
            df = pd.read_excel(io.BytesIO(content))

    config = generate_config(prompt, target_format)
    dag = generate_dag(prompt)
    transformed = transform_data(df, prompt) if df is not None else None

    return {
        "config": config,
        "dag": dag,
        "transformed_data": transformed.to_dict(orient="records") if transformed is not None else []
    }

@app.post("/smart-fix")
async def smart_fix(error_message: str = Form(...)):
    suggestion = get_smart_fix(error_message)
    return {"fix_suggestion": suggestion}

