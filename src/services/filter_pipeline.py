# src/services/filter_pipeline.py
from typing import List, Dict, Any, Iterable, Optional
from src.classify.service import moderate_text

POSSIBLE_TEXT_FIELDS = ["text", "comment", "content", "body", "message", "title"]

def _detect_text_field(rec: Dict[str, Any], fallback: str = "text") -> str:
    for k in POSSIBLE_TEXT_FIELDS:
        if k in rec and isinstance(rec[k], str):
            return k
    return fallback

def _include(action: str, min_action: str) -> bool:
    order = {"allow": 0, "review": 1, "block": 2}
    return order.get(action, 0) >= order.get(min_action, 1)

def filter_records(
    records: Iterable[Dict[str, Any]],
    min_action: str = "review",      # "review" (review+block) ou "block"
    text_field: Optional[str] = None # force o campo se quiser
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for rec in records:
        field = text_field or _detect_text_field(rec)
        txt = rec.get(field, "")
        if not isinstance(txt, str) or not txt.strip():
            continue
        mod = moderate_text(txt)
        if _include(mod["action"], min_action):
            rec["_moderation"] = {
                "action": mod["action"], "label": mod["label"], "final_score": mod["final_score"],
                "rules": mod["rules"], "semantic": mod["semantic"],
            }
            out.append(rec)
    return out
