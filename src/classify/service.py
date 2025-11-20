# src/classify/service.py
from functools import lru_cache
from typing import Dict, Any, Optional

from src.preprocess.text import Preprocessor
from src.rules.filter import compile_regex_patterns, apply_rules
from src.classify.aggregator import aggregate_risk
from src.services.vocab_client import fetch_vocab
from src.services.perspective import get_sexually_explicit_score

# Pesos/limiares padrão (ajuste à vontade)
BASE_THRESHOLDS: Dict[str, float] = {
    "rule_weight": 0.9,      # mais peso nas regras (MVP sem encoder)
    "semantic_weight": 0.0,  # 0.0 se você desligou o encoder
    "similarity": 0.55,
    "decision": 0.90,
}

@lru_cache(maxsize=1)
def _pre() -> Preprocessor:
    return Preprocessor("pt_core_news_sm")

@lru_cache(maxsize=1)
def _vocab() -> Dict[str, Any]:
    # busca do vocabulário (remoto via VOCAB_API_URL ou localhost)
    return fetch_vocab()

@lru_cache(maxsize=1)
def _compiled_regex():
    vocab = _vocab()
    return compile_regex_patterns(vocab.get("regex_patterns", {}))

def moderate_text(
    text: str,
    thresholds: Optional[Dict[str, float]] = None,
    lang: str = "pt",
    use_perspective: bool = True,
) -> Dict[str, Any]:
    """
    Liga: preprocess -> regras -> (encoder desligado) -> aggregator (+optional Perspective)
    Retorna: score, label, action, hits e metadados.
    """
    th = {**BASE_THRESHOLDS, **(thresholds or {})}
    vocab = _vocab()

    preprocessed = _pre().preprocess(text)

    hits = apply_rules(
        text_original=text,
        text_preprocessed=preprocessed,
        keywords_explicit=vocab.get("keywords_explicit", []),
        regex_compiled=_compiled_regex(),
    )

    # 3) similaridade semântica (desligada aqui)
    semantic_sim = 0.0

    # perspective_val = get_sexually_explicit_score(text, lang=lang) if use_perspective else None

    if len(hits) >= 2 and th["rule_weight"] < 1.0:
        th = {**th, "rule_weight": 0.95}

    final_score, label = aggregate_risk(
        rule_hits=hits,
        semantic_score=semantic_sim,
        thresholds=th,
        # perspective_sexual=perspective_val,
        perspective_weight=0.4
    )

    action = "block" if label == "suspeito" else ("review" if label == "atencao" else "allow")

    return {
        "input_text": text,
        "preprocessed": preprocessed,
        "rules": {"hits": hits, "count": len(hits)},
        "semantic": {"similarity": semantic_sim},
        # "perspective": {"sexually_explicit": perspective_val},
        "thresholds": th,
        "vocab_version": vocab.get("version", "unknown"),
        "final_score": final_score,
        "label": label,
        "action": action,
    }
