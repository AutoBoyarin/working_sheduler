import os
import json
import shutil
import time
import argparse
from urllib.parse import urlparse

from .text_moderator.text_moderator import moderate_text
from .image_moderator.image_moderator import moderate_images

from .config import load_config
from .logging_setup import setup_logging
from .db import (
    init_db,
    get_conn,
    fetch_paid_ads,
    group_ads,
    save_run,
    save_detections,
    save_result_summary,
    commit_ad_moderated,
    commit_ad_rejected,
    replace_advertisement_images,
)
from .storage import _make_client, ensure_bucket, upload_file, build_object_url
from .utils import download_files

# Параметры путей берутся из конфигурации (см. config.py)
def run_once(cfg):
    output_folder = cfg.output_folder
    model_path = cfg.model_path
    os.makedirs(output_folder, exist_ok=True)
    # По флагу из .env очищаем выходную папку перед запуском
    if getattr(cfg, "clean_output_on_start", False):
        try:
            shutil.rmtree(output_folder, ignore_errors=True)
        except Exception:
            pass
        os.makedirs(output_folder, exist_ok=True)

    # Инициализация БД и MinIO
    init_db(cfg.db)
    minio_client = _make_client(cfg.minio)
    ensure_bucket(minio_client, cfg.minio.system_bucket, public=False)
    ensure_bucket(minio_client, cfg.minio.client_bucket, public=cfg.minio.client_public_access)

    with get_conn(cfg.db) as conn:
        rows = fetch_paid_ads(conn, limit=cfg.batch_limit)
        ads = group_ads(rows)

        for ad_id, data in ads.items():
            description = data.get("description") or ""
            image_urls = data.get("image_urls") or []

            verdict = {"acceptable": True, "detections": []}

            # Текстовая модерация
            if description:
                verdict["detections"].extend(moderate_text(description))

            # Скачиваем изображения во временную папку
            tmp_dir = os.path.join(output_folder, "tmp", ad_id)
            local_paths = download_files(image_urls, tmp_dir)

            # Запускаем модерацию изображений
            if local_paths:
                covered_dir = os.path.join(output_folder, "images")
                img_dets = moderate_images(
                    image_paths=local_paths,
                    model_path=model_path,
                    output_dir=covered_dir,
                    ad_id=ad_id,
                )
                verdict["detections"].extend(img_dets)

                # Загружаем покрытые изображения в MinIO и собираем новые ссылки
                uploaded_object_keys = []
                seen_paths = set()
                for det in img_dets:
                    out_path = det.get("output_path")
                    if not out_path:
                        continue
                    # Загружаем файл ровно один раз на уникальный out_path
                    if out_path not in seen_paths:
                        seen_paths.add(out_path)
                        filename = os.path.basename(out_path)
                        object_name = f"images/covered/{ad_id}/{filename}"
                        upload_file(minio_client, cfg.minio.client_bucket, out_path, object_name)
                        uploaded_object_keys.append(object_name)
                    else:
                        # Вычисляем object_name по существующему пути
                        filename = os.path.basename(out_path)
                        object_name = f"images/covered/{ad_id}/{filename}"
                    # Проставляем object_key всем детекциям
                    det["object_key"] = object_name

                # Формируем публичные или s3-ссылки и заменяем их в advertisement_images
                if uploaded_object_keys:
                    if cfg.minio.client_public_access:
                        new_urls = [
                            build_object_url(cfg.minio, cfg.minio.client_bucket, key)
                            for key in uploaded_object_keys
                        ]
                    else:
                        # Для приватных бакетов сохраняем canonical s3-ссылку
                        new_urls = [f"s3://{cfg.minio.client_bucket}/{key}" for key in uploaded_object_keys]

                    try:
                        replace_advertisement_images(conn, ad_id, new_urls)
                    except Exception as e:
                        print(f"[DB][ERROR] Failed to replace images for ad {ad_id}: {e}")

            # Итог и сохранение в наши таблицы
            if verdict["detections"]:
                verdict["acceptable"] = False

            run_id = save_run(conn, verdict["acceptable"], ad_id, verdict)
            save_detections(conn, run_id, verdict["detections"])
            # Сводная запись по результатам модерации (отдельная таблица)
            save_result_summary(conn, run_id, ad_id, verdict["detections"]) 

            # По флагу COMMIT_RESULTS: если есть нарушения в тексте — REJECTED, иначе MODERATED
            if getattr(cfg, "commit_results", False):
                try:
                    has_text_violations = any(d.get("type") == "text" for d in verdict["detections"])
                    if has_text_violations:
                        updated = commit_ad_rejected(conn, ad_id)
                        status_str = "REJECTED"
                    else:
                        updated = commit_ad_moderated(conn, ad_id)
                        status_str = "MODERATED"

                    if updated:
                        print(f"[COMMIT] Ad {ad_id}: status -> {status_str} (rows updated: {updated})")
                    else:
                        print(f"[COMMIT] Ad {ad_id}: no rows updated (possibly not in PAID)")
                except Exception as e:
                    print(f"[COMMIT][ERROR] Failed to update ad {ad_id}: {e}")

            # Очистка временных файлов
            try:
                shutil.rmtree(tmp_dir, ignore_errors=True)
            except Exception:
                pass

            # Локальный вывод результата для отладки
            out_json = os.path.join(output_folder, f"verdict_{ad_id}.json")
            with open(out_json, "w", encoding="utf-8") as f:
                json.dump(verdict, f, ensure_ascii=False, indent=2)

    print("=== BATCH MODERATION DONE ===")


def main():
    parser = argparse.ArgumentParser(description="Ad moderation runner")
    parser.add_argument(
        "-i",
        "--interval-minutes",
        dest="interval_minutes",
        type=int,
        default=None,  # None позволит отличить "не задано" от 0 в .env
        help="Периодичность запуска в минутах. 0 — однократный запуск.",
    )
    args = parser.parse_args()

    cfg = load_config()
    # Инициализация логирования до любой логики
    try:
        setup_logging(cfg.log)
    except Exception:
        # В крайнем случае не падаем из‑за логгера
        pass

    # Приоритет: CLI (-i) > .env (SCHEDULER_INTERVAL_MINUTES) > 0 по умолчанию
    interval_minutes = (
        args.interval_minutes
        if args.interval_minutes is not None
        else getattr(cfg, "scheduler_interval_minutes", 0)
    )

    if interval_minutes and interval_minutes > 0:
        interval_sec = interval_minutes * 60
        print(f"[SCHEDULER] Запуск в цикле каждые {interval_minutes} мин. Нажмите Ctrl+C для остановки.")
        try:
            while True:
                start_ts = time.strftime("%Y-%m-%d %H:%M:%S")
                print(f"[SCHEDULER] Старт задачи: {start_ts}")
                run_once(cfg)
                print(f"[SCHEDULER] Сон {interval_minutes} мин...")
                time.sleep(interval_sec)
        except KeyboardInterrupt:
            print("[SCHEDULER] Остановка по запросу пользователя (Ctrl+C)")
    else:
        run_once(cfg)


if __name__ == "__main__":
    main()
