from fastapi import FastAPI
from app.fetch_data import router as fetch_data_router
from app.get_trends import router as get_trends_router
from fastapi.middleware.cors import CORSMiddleware



app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # This allows all origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(fetch_data_router)
app.include_router(get_trends_router)
