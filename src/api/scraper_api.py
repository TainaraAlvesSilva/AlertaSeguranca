from fastapi import FastAPI, Body
import pandas as pd

# uvicorn src.api.scraper_api:app --reload
# http://127.0.0.1:8000/docs#/default/

# importa funções dos scrapers
from src.ingestion.twitter_web import (
    scrape_twitter_profile,
    scrape_twitter_hashtag,
    scrape_twitter_post,
)

from src.ingestion.instagram_web import (
    scrape_instagram_one,
    scrape_instagram_much
)

from src.ingestion.youtube import get_youtube_comments
from src.ingestion.reddit import get_reddit_comments

from src.services.filter_pipeline import filter_records

app = FastAPI(title="Scraper API", version="1.0.0")

# Helper: converte DataFrame para JSON
def df_to_json(df: pd.DataFrame):
    return df.to_dict(orient="records")

# ---------------------- TWITTER ----------------------

@app.get("/twitter/web")
def twitter_web(mode: str, query: str, limit: int = 10):
    if mode == "profile":
        data = scrape_twitter_profile(query, limit)
    elif mode == "hashtag":
        data = scrape_twitter_hashtag(query, limit)
    elif mode == "post":
        data = scrape_twitter_post(query, limit)
    else:
        return {"error": "Modo inválido. Use: profile, hashtag ou post"}
    if isinstance(data, pd.DataFrame):
        data = df_to_json(data)
    return filter_records(data)

# ---------------------- INSTAGRAM --------------------

@app.get("/instagram/web/one")
def instagram_web_one(user: str, password: str, mode: str, id: str, limit: int = 10):
    data = scrape_instagram_one(user, password, mode, id, limit)
    if isinstance(data, pd.DataFrame):
        data = df_to_json(data)
    return filter_records(data)
    
@app.post("/instagram/web/much")
def instagram_web_much(user: str, password: str, body: dict = Body(...), limit: int = 10):
    data = scrape_instagram_much(user, password, body, limit)
    if isinstance(data, pd.DataFrame):
        data = df_to_json(data) 
    return filter_records(data)

# ---------------------- YOUTUBE ----------------------

@app.get("/scraper/youtube")
def youtube_comments(url: str, limit: int = 20):
    df = get_youtube_comments(url, limit=limit)
    return df_to_json(df)

# ---------------------- REDDIT ----------------------

@app.get("/scraper/reddit")
def reddit_comments(url: str, limit: int = 20):
    df = get_reddit_comments(url, limit=limit)
    return df_to_json(df)
