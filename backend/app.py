from fastapi import FastAPI, UploadFile, Form, HTTPException
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
    try:
        df = None
        if file:
            content = await file.read()
            if file.filename.endswith(".csv"):
                df = pd.read_csv(io.BytesIO(content))
            elif file.filename.endswith((".xlsx", ".xls")):
                df = pd.read_excel(io.BytesIO(content))
            else:
                raise HTTPException(status_code=400, detail="Unsupported file format")

        # Generate configuration in backend
        config = generate_config(prompt, target_format)
        
        # Generate DAG diagram
        dag = generate_dag(prompt)
        
        # Transform data if file provided
        transformed = None
        if df is not None:
            transformed = transform_data(df, prompt)

        return {
            "success": True,
            "config": config,
            "dag": dag,
            "transformed_data": transformed.to_dict(orient="records") if transformed is not None else [],
            "original_data": df.to_dict(orient="records") if df is not None else []
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing workflow: {str(e)}")

@app.post("/smart-fix")
async def smart_fix(error_message: str = Form(...)):
    try:
        suggestion = get_smart_fix(error_message)
        return {"success": True, "fix_suggestion": suggestion}
    except Exception as e:
        return {"success": False, "error": f"Failed to generate fix: {str(e)}"}