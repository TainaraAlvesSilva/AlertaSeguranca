"""
src/ingestion/youtube.py
"""

from typing import List, Dict, Tuple
from googleapiclient.discovery import build
import time
import logging

logger = logging.getLogger("ingestion.youtube")

def fetch_comments(
    video_id: str,
    api_key: str,
    max_pages: int = 3,
    page_size: int = 100
) -> List[Dict]:
    """
    Busca comentários usando a YouTube Data API v3.
    - video_id: ID do vídeo (ex.: 'dQw4w9WgXcQ')
    - max_pages: quantas páginas (paginação)
    - page_size: até 100 por página
    Retorna: lista de itens crus da API.
    """
    youtube = build("youtube", "v3", developerKey=api_key)
    comments_raw: List[Dict] = []

    request = youtube.commentThreads().list(
        part="snippet",
        videoId=video_id,
        maxResults=min(page_size, 100),
        textFormat="plainText",
        order="time"
    )

    page = 0
    while request and page < max_pages:
        response = request.execute()
        items = response.get("items", [])
        comments_raw.extend(items)
        logger.info(f"[YouTube] Página {page+1}: {len(items)} comentários")
        request = youtube.commentThreads().list_next(request, response)
        page += 1
        time.sleep(0.3)  # evita throttling

    return comments_raw

def normalize_comment(item: Dict) -> Tuple[str, Dict]:
    """
campos que serão retornador Retorna (comment_id, payload_normalizado).
    """
    snippet = item.get("snippet", {})
    top = snippet.get("topLevelComment", {}).get("snippet", {})

    comment_id = item.get("id", "")
    author = top.get("authorDisplayName")
    text = top.get("textDisplay", "")  # plainText
    like_count = top.get("likeCount", 0)
    published_at = top.get("publishedAt")

    payload = {
        "author": author,
        "text": text,
        "likeCount": like_count,
        "publishedAt": published_at
    }
    return comment_id, payload
