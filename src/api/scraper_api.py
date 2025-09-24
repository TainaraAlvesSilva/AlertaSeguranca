import pwd
from fastapi import FastAPI, Query
from typing import Optional
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
    scrape_instagram_post,
    scrape_instagram_reels
)

from src.ingestion.youtube import get_youtube_comments
from src.ingestion.reddit import get_reddit_comments

app = FastAPI(title="Scraper API", version="1.0.0")

# Helper: converte DataFrame para JSON
def df_to_json(df: pd.DataFrame):
    return df.to_dict(orient="records")

# ---------------------- TWITTER ----------------------

@app.get("/twitter/web")
def twitter_web(mode: str, query: str, limit: int = 10):
    if mode == "profile":
        return scrape_twitter_profile(query, limit)
    elif mode == "hashtag":
        return scrape_twitter_hashtag(query, limit)
    elif mode == "post":
        return scrape_twitter_post(query, limit)
    else:
        return {"error": "Modo inválido. Use: profile, hashtag ou post"}

# ---------------------- INSTAGRAM --------------------

@app.get("/instagram/web")
def instagram_web(user: str, password: str, mode: str, id: str, limit: int = 10):
    if mode == "post":
        return scrape_instagram_post(user, password, id, limit)
    elif mode == "reel":
        return scrape_instagram_reels(user, password, id, limit)
    else:
        return {"error": "Modo inválido. Use: post ou reels"}

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
