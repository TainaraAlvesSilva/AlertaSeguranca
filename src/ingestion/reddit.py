# src/ingestion/reddit.py
from datetime import datetime
from typing import List, Dict
import praw

from .reddit_client import get_client

def fetch_submission_comments(
    submission_id: str,
    limit: int = 200,
    sort: str = "new",
    only_root: bool = False,
) -> List[Dict]:

    reddit = get_client()
    sub = reddit.submission(id=submission_id)

    # sort aceitos pelo Reddit
    if sort in {"new", "top", "best", "controversial", "old", "qa"}:
        sub.comment_sort = sort

    # expande 'MoreComments'
    sub.comments.replace_more(limit=0)

    # lista alvo (todos comentários ou apenas top-level)
    comments = sub.comments if only_root else sub.comments.list()

    out: List[Dict] = []
    count = 0
    for c in comments:
        # filtra apenas Comment e (se solicitado) apenas top-level
        if not isinstance(c, praw.models.Comment):
            continue
        if only_root and not c.is_root:
            continue

        author = str(c.author) if c.author else "[deleted]"
        out.append({
            "comment_id": f"{sub.id}:{c.id}",
            "platform": "reddit",
            "source_id": sub.id,
            "author": author,
            "text": c.body or "",
            "likeCount": getattr(c, "score", 0),
            "publishedAt": datetime.utcfromtimestamp(c.created_utc).isoformat() + "Z",
            "permalink": f"https://www.reddit.com{c.permalink}",
        })

        count += 1
        if limit and count >= limit:
            break

    return out
