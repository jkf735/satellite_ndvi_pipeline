import os
os.environ["AWS_NO_SIGN_REQUEST"] = "YES"

from titiler.application.main import app
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)