from fastapi import FastAPI
from pydantic import BaseModel
import json
from pathlib import Path

app = FastAPI(title="Vocab API", version="0.1.0")
VOCAB_PATH = Path(__file__).parent / "vocab.json"

class VocabResponse(BaseModel):
    keywords_explicit: list[str]
    examples_implicit: list[str]
    regex_patterns: dict
    version: str

@app.get("/v1/vocab", response_model=VocabResponse)
def get_vocab():
    data = json.loads(VOCAB_PATH.read_text(encoding="utf-8"))
    return data
