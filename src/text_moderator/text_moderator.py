import os
from typing import List, Optional

# ---------- Параметры ----------
# Локальный путь к модели токсичности
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TOXIC_MODEL_PATH = os.path.join(BASE_DIR, "russian_toxicity_classifier")

# Zero-shot модель (можно оставить онлайн или тоже скачать локально)
ZERO_SHOT_MODEL = "facebook/bart-large-mnli"

# Категории для zero-shot
ZS_LABELS = ["trash_talk", "politics", "crypto", "acceptable"]

# Порог вероятности
THRESHOLD = 0.6

# ---------- Ленивая инициализация пайплайнов ----------
_tox_classifier = None
_zs_classifier = None


def _import_pipeline():
    try:
        from transformers import pipeline  # type: ignore
        return pipeline
    except Exception:
        return None


def _get_tox_classifier():
    global _tox_classifier
    if _tox_classifier is not None:
        return _tox_classifier
    pipeline = _import_pipeline()
    if pipeline is None:
        return None
    try:
        _tox_classifier = pipeline(
            "text-classification",
            model=TOXIC_MODEL_PATH,
            tokenizer=TOXIC_MODEL_PATH,
            return_all_scores=True,
        )
    except Exception:
        _tox_classifier = None
    return _tox_classifier


def _get_zs_classifier():
    global _zs_classifier
    if _zs_classifier is not None:
        return _zs_classifier
    # Делаем zero-shot опциональным: можно отключить через переменную окружения
    zs_enabled = str(os.environ.get("TEXT_ZEROSHOT_ENABLED", "0")).strip().lower() in {"1", "true", "yes", "on", "y"}
    if not zs_enabled:
        return None
    pipeline = _import_pipeline()
    if pipeline is None:
        return None
    try:
        _zs_classifier = pipeline(
            "zero-shot-classification",
            model=ZERO_SHOT_MODEL,
        )
    except Exception:
        _zs_classifier = None
    return _zs_classifier

# ---------- Функции модерации текста ----------
def moderate_text_ai(text: str) -> List[dict]:
    """
    Возвращает список детекций для текста:
    - type: "text"
    - category: trash_talk | politics | crypto
    - score: вероятность
    - value: исходный текст / токен
    """
    detections: List[dict] = []

    # Небольшая предобработка
    text_norm = " ".join((text or "").split())
    if not text_norm:
        return []

    threshold_str = os.environ.get("TEXT_THRESHOLD")
    try:
        threshold = float(threshold_str) if threshold_str else THRESHOLD
    except Exception:
        threshold = THRESHOLD

    # -------- Токсичность (локальная модель, если доступна) --------
    tox = _get_tox_classifier()
    if tox is not None:
        try:
            tox_results = tox(text_norm)
            if isinstance(tox_results, list) and tox_results:
                for r in tox_results[0]:  # tox_results возвращает список списков
                    label = str(r.get('label', '')).lower()
                    score = float(r.get('score', 0.0))
                    if label and label != "not_toxic" and score > threshold:
                        detections.append({
                            "type": "text",
                            "category": "trash_talk",
                            "score": score,
                            "value": text_norm,
                        })
        except Exception:
            pass
    else:
        # Фолбэк-правила для токсичности
        toxic_keywords = [
            "идиот", "дурак", "тупой", "сволочь", "ублюд", "сука", "бляд", "лох",
        ]
        if any(k in text_norm.lower() for k in toxic_keywords):
            detections.append({
                "type": "text",
                "category": "trash_talk",
                "score": 1.0,
                "value": text_norm,
            })

    # -------- Тематическая классификация (zero-shot, если включена и доступна) --------
    zs = _get_zs_classifier()
    if zs is not None:
        try:
            zs_labels_env = os.environ.get("TEXT_ZS_LABELS")
            labels = [s.strip() for s in zs_labels_env.split(",") if s.strip()] if zs_labels_env else ZS_LABELS
            zs_result = zs(text_norm, labels)
            labels_out = zs_result.get("labels", [])
            scores_out = zs_result.get("scores", [])
            for label, score in zip(labels_out, scores_out):
                if label != "acceptable" and float(score) > threshold:
                    detections.append({
                        "type": "text",
                        "category": label,
                        "score": float(score),
                        "value": text_norm,
                    })
        except Exception:
            pass
    else:
        # Фолбэк-правила по ключевым словам
        lowered = text_norm.lower()
        rules = [
            ("politics", ["путин", "выборы", "митинг", "депутат", "рада", "кремль", "полит"]),
            ("crypto", ["биткоин", "bitcoin", "крипто", "ethereum", "эфир", "bnb", "usdt"]),
        ]
        for category, keywords in rules:
            if any(k in lowered for k in keywords):
                detections.append({
                    "type": "text",
                    "category": category,
                    "score": 1.0,
                    "value": text_norm,
                })

    return detections


def moderate_text(text: str) -> List[dict]:
    """Совместимая обёртка, ожидаемая остальным кодом проекта.

    Возвращает список детекций по тексту. В случае ошибок внутри
    NLP-пайплайнов аккуратно возвращает пустой список, чтобы не
    ронять весь процесс модерации.
    """
    if not text:
        return []
    try:
        return moderate_text_ai(text)
    except Exception:
        # Фэйл-сейф: на любых ошибках возвращаем пустой список
        return []


# ---------- Пример использования ----------
if __name__ == "__main__":
    test_texts = [
        "Ты полный идиот",
        "Биткоин скоро взлетит",
        "Путин снова выступил на митинге"
    ]

    for t in test_texts:
        det = moderate_text_ai(t)
        if det:
            print(f"❌ Текст '{t}' НЕприемлемый:")
            for d in det:
                print(f"   - Категория: {d['category']}, вероятность: {d['score']:.2f}")
        else:
            print(f"✅ Текст '{t}' приемлемый")
