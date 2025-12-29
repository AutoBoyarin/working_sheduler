import os
import json
import shutil
from urllib.parse import urlparse

from .text_moderator.text_moderator import moderate_text
from .image_moderator.image_moderator import moderate_images

from .config import load_config
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
)
from .storage import _make_client, ensure_bucket, upload_file
from .utils import download_files

# ========== ПАРАМЕТРЫ ==========
OUTPUT_FOLDER = r"C:\Code\Python\working_sheduler\src\image_moderator\output"
MODEL_PATH = r"C:\Code\Python\working_sheduler\src\image_moderator\models\license-plate-finetune-v1l.onnx"

os.makedirs(OUTPUT_FOLDER, exist_ok=True)


def main():
    cfg = load_config()

    # По флагу из .env очищаем выходную папку перед запуском
    if getattr(cfg, "clean_output_on_start", False):
        try:
            shutil.rmtree(OUTPUT_FOLDER, ignore_errors=True)
        except Exception:
            pass
        os.makedirs(OUTPUT_FOLDER, exist_ok=True)

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
            tmp_dir = os.path.join(OUTPUT_FOLDER, "tmp", ad_id)
            local_paths = download_files(image_urls, tmp_dir)

            # Запускаем модерацию изображений
            if local_paths:
                covered_dir = os.path.join(OUTPUT_FOLDER, "images")
                img_dets = moderate_images(
                    image_paths=local_paths,
                    model_path=MODEL_PATH,
                    output_dir=covered_dir,
                    ad_id=ad_id,
                )
                verdict["detections"].extend(img_dets)

                # Загружаем покрытые изображения в MinIO
                for det in img_dets:
                    out_path = det.get("output_path")
                    if not out_path:
                        continue
                    filename = os.path.basename(out_path)
                    object_name = f"images/covered/{ad_id}/{filename}"
                    upload_file(minio_client, cfg.minio.client_bucket, out_path, object_name)
                    det["object_key"] = object_name

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
            out_json = os.path.join(OUTPUT_FOLDER, f"verdict_{ad_id}.json")
            with open(out_json, "w", encoding="utf-8") as f:
                json.dump(verdict, f, ensure_ascii=False, indent=2)

    print("=== BATCH MODERATION DONE ===")


if __name__ == "__main__":
    main()
