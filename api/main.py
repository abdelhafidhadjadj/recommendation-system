from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Recommandation Scientifique USTHB")

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/")
def root():
    return {"message": "API opérationnelle", "status": "running"}

@app.get("/health")
def health():
    return {"status": "healthy"}
