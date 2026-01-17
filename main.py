from fastapi import FastAPI
from contextlib import asynccontextmanager
import os
import sys
import uvicorn
import logging
from fastapi.middleware.cors import CORSMiddleware
from api.generate_sql import router as sql_router

# sys.path.append(os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # port = os.getenv("port")
    print(f"🚀 FastAPI started at http://localhost:8000")
    yield
    logger.info(" FastAPI shutting down")

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods
    allow_headers=["*"],  # Allow all headers
)

@app.head("/")
def health_check():
    return {"status": "ok"}


@app.get("/")
def home():
    return {"message": "Server is live"}

app.include_router(sql_router,prefix='/api')

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)

