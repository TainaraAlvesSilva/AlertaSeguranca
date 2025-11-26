import os
import pandas as pd
import re
from datetime import datetime
from typing import List, Dict, Any

CSV_COLUMNS = ["platform", "comment"]

def sanitize_filename(text: str) -> str:
    text = re.sub(r"[^a-zA-Z0-9_-]+", "_", text)
    return text[:80]

def ensure_minimal_columns(df: pd.DataFrame, platform: str) -> pd.DataFrame:
    df = df.copy()
    df["platform"] = platform

    if "comment" not in df.columns:
        if "text" in df.columns:
            df["comment"] = df["text"]
        else:
            df["comment"] = None
    return df[CSV_COLUMNS]


def save_csv(df: pd.DataFrame, platform: str, identifier: str, kind: str = None, base_dir="data/out"):
    if df.empty:
        return None
    
    safe_id = sanitize_filename(identifier)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if kind:
        folder = os.path.join(base_dir, platform, kind)
    else:
        folder = os.path.join(base_dir, platform)

    os.makedirs(folder, exist_ok=True)

    file_path = os.path.join(folder, f"{safe_id}_{timestamp}.csv")
    df_norm = ensure_minimal_columns(df, platform)
    df_norm.to_csv(file_path, index=False, encoding="utf-8")

    return file_path

def export_comments_batch(
    items: List[Dict[str, Any]],
    platform: str,
    kind: str | None,
    save: bool = False,
    base_dir: str = "data/out",
) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []

    for item in items:
        item_id = item.get("id")
        comments = item.get("data") or []

        csv_path = None
        if save and comments:
            df = pd.DataFrame.from_records(comments)
            csv_path = save_csv(
                df=df,
                platform=platform,
                identifier=item_id,
                kind=kind,
                base_dir=base_dir,
            )

        results.append({
            "id": item_id,
            "comments_count": len(comments),
            "csv": csv_path,
        })

    return results