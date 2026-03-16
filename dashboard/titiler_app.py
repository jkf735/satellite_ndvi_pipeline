"""
titiler_app.py
Local Titiler tile server for serving COGs from S3.

Usage:
    uvicorn dashboard.titiler_app:app --host localhost --port 8001
    make titiler
"""
import os
from dotenv import load_dotenv
from titiler.application.main import app
from titiler.core.errors import DEFAULT_STATUS_CODES, add_exception_handlers

load_dotenv()
os.environ["AWS_NO_SIGN_REQUEST"] = "YES"
# configure CORS so Streamlit can call Titiler
from fastapi.middleware.cors import CORSMiddleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8501"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

add_exception_handlers(app, DEFAULT_STATUS_CODES)