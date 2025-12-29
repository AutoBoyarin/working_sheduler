from __future__ import annotations

import json
from typing import Iterable, List, Dict, Tuple, Optional

import psycopg

from .config import DbConfig


def get_conn(cfg: DbConfig) -> psycopg.Connection:
    return psycopg.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        dbname=cfg.name,
    )


def init_db(cfg: DbConfig) -> None:
    ddl_runs = (
        """
        CREATE TABLE IF NOT EXISTS moderation_runs (
            id BIGSERIAL PRIMARY KEY,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            acceptable BOOLEAN NOT NULL,
            source_id TEXT,
            verdict_json JSONB
        );
        """
    )

    ddl_detections = (
        """
        CREATE TABLE IF NOT EXISTS moderation_detections (
            id BIGSERIAL PRIMARY KEY,
            run_id BIGINT NOT NULL REFERENCES moderation_runs(id) ON DELETE CASCADE,
            type TEXT,
            category TEXT,
            value TEXT,
            image_path TEXT,
            object_key TEXT
        );
        """
    )

    ddl_results = (
        """
        CREATE TABLE IF NOT EXISTS moderation_results (
            id BIGSERIAL PRIMARY KEY,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            ad_id TEXT NOT NULL,
            run_id BIGINT NOT NULL REFERENCES moderation_runs(id) ON DELETE CASCADE,
            acceptable BOOLEAN NOT NULL,
            text_acceptable BOOLEAN NOT NULL,
            image_acceptable BOOLEAN NOT NULL,
            total_detections INT NOT NULL DEFAULT 0,
            text_detections INT NOT NULL DEFAULT 0,
            image_detections INT NOT NULL DEFAULT 0,
            text_summary JSONB,
            image_summary JSONB
        );
        """
    )

    with get_conn(cfg) as conn:
        with conn.cursor() as cur:
            cur.execute(ddl_runs)
            cur.execute(ddl_detections)
            cur.execute(ddl_results)
        conn.commit()


def health_check(cfg: DbConfig) -> bool:
    try:
        with get_conn(cfg) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                _ = cur.fetchone()
        return True
    except Exception:
        return False


def fetch_paid_ads(conn: psycopg.Connection, limit: Optional[int] = None) -> List[Tuple[str, str, str]]:
    sql = (
        """
        select au.id, au.description, ai.image_url
        from advertisement_auto au
        inner join public.advertisement_images ai on au.id = ai.advertisement_id
        where au.status = 'PAID'
        order by au.created_at asc
        """
    )
    if limit is not None:
        sql += " limit %s"
        with conn.cursor() as cur:
            cur.execute(sql, (limit,))
            return [(str(r[0]), r[1], r[2]) for r in cur.fetchall()]
    else:
        with conn.cursor() as cur:
            cur.execute(sql)
            return [(str(r[0]), r[1], r[2]) for r in cur.fetchall()]


def group_ads(rows: Iterable[Tuple[str, str, str]]) -> Dict[str, Dict[str, object]]:
    grouped: Dict[str, Dict[str, object]] = {}
    for ad_id, description, image_url in rows:
        g = grouped.setdefault(ad_id, {"description": description, "image_urls": []})
        # На случай, если у одной записи пустое description, сохраняем непустое
        if not g.get("description") and description:
            g["description"] = description
        if image_url:
            g["image_urls"].append(image_url)
    return grouped


def save_run(conn: psycopg.Connection, acceptable: bool, source_id: str, verdict_json: dict) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO moderation_runs(acceptable, source_id, verdict_json)
            VALUES (%s, %s, %s)
            RETURNING id
            """,
            (acceptable, source_id, json.dumps(verdict_json, ensure_ascii=False)),
        )
        run_id = cur.fetchone()[0]
    conn.commit()
    return int(run_id)


def save_detections(conn: psycopg.Connection, run_id: int, items: List[dict]) -> None:
    if not items:
        return
    rows = []
    for it in items:
        rows.append(
            (
                run_id,
                it.get("type"),
                it.get("category"),
                it.get("value"),
                it.get("image"),
                it.get("object_key"),
            )
        )
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO moderation_detections(run_id, type, category, value, image_path, object_key)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            rows,
        )
    conn.commit()


def save_result_summary(
    conn: psycopg.Connection,
    run_id: int,
    ad_id: str,
    detections: List[dict],
) -> int:
    """Сохраняет агрегированный результат модерации в таблицу moderation_results.

    Расчёт:
    - text_detections: количество детекций с type == 'text'
    - image_detections: количество детекций с type == 'image'
    - acceptable = (text_detections == 0 and image_detections == 0)
    - text_summary: JSON по категориям с уникальными values
    - image_summary: JSON по категориям с перечнем изображений/ключей
    """
    text_count = 0
    image_count = 0

    text_summary: Dict[str, Dict[str, object]] = {}
    image_summary: Dict[str, Dict[str, object]] = {}

    for d in detections or []:
        d_type = d.get("type")
        category = d.get("category") or "unknown"

        if d_type == "text":
            text_count += 1
            entry = text_summary.setdefault(category, {"values": set(), "count": 0})
            val = d.get("value")
            if val:
                entry["values"].add(str(val))
            entry["count"] = int(entry.get("count", 0)) + 1
        elif d_type == "image":
            image_count += 1
            entry = image_summary.setdefault(category, {"items": [], "count": 0})
            item = {"image": d.get("image"), "object_key": d.get("object_key")}
            entry["items"].append(item)
            entry["count"] = int(entry.get("count", 0)) + 1

    # Преобразуем множества в списки для JSON
    for cat, e in text_summary.items():
        if isinstance(e.get("values"), set):
            e["values"] = sorted(list(e["values"]))

    acceptable = (text_count == 0 and image_count == 0)
    text_acceptable = (text_count == 0)
    image_acceptable = (image_count == 0)

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO moderation_results (
                ad_id, run_id, acceptable, text_acceptable, image_acceptable,
                total_detections, text_detections, image_detections,
                text_summary, image_summary
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                str(ad_id),
                run_id,
                acceptable,
                text_acceptable,
                image_acceptable,
                int(text_count + image_count),
                int(text_count),
                int(image_count),
                json.dumps(text_summary, ensure_ascii=False),
                json.dumps(image_summary, ensure_ascii=False),
            ),
        )
        res_id = cur.fetchone()[0]
    conn.commit()
    return int(res_id)


def commit_ad_moderated(conn: psycopg.Connection, ad_id: str) -> int:
    """Переводит объявление в статус MODERATED и проставляет дату `moderated_at`.

    Меняем только те записи, которые ещё в статусе PAID, чтобы избежать лишних апдейтов.
    Возвращает количество обновлённых строк.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE advertisement_auto
            SET status = 'MODERATED',
                moderated_at = NOW()
            WHERE id = %s AND status = 'PAID'
            """,
            (ad_id,),
        )
        affected = cur.rowcount or 0
    conn.commit()
    return int(affected)


def commit_ad_rejected(conn: psycopg.Connection, ad_id: str) -> int:
    """Переводит объявление в статус REJECTED и проставляет дату `moderated_at`.

    Меняем только записи со статусом PAID. Возвращает количество обновлённых строк.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE advertisement_auto
            SET status = 'REJECTED',
                moderated_at = NOW()
            WHERE id = %s AND status = 'PAID'
            """,
            (ad_id,),
        )
        affected = cur.rowcount or 0
    conn.commit()
    return int(affected)
