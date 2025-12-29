from __future__ import annotations

import datetime as dt
import logging
import os
from urllib.parse import urlparse

from minio import Minio
from minio.error import S3Error

from .config import MinioConfig


def _make_client(cfg: MinioConfig) -> Minio:
    parsed = urlparse(cfg.internal_url)
    secure = parsed.scheme == "https"
    endpoint = parsed.netloc
    return Minio(
        endpoint=endpoint,
        access_key=cfg.access_key,
        secret_key=cfg.secret_key,
        secure=secure,
    )


def ensure_bucket(client: Minio, bucket: str, public: bool = False) -> None:
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
    if public:
        # Простая политика read-only для всех объектов бакета
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": ["*"]},
                    "Action": ["s3:GetObject"],
                    "Resource": [f"arn:aws:s3:::{bucket}/*"],
                }
            ],
        }
        import json

        client.set_bucket_policy(bucket, json.dumps(policy))


def upload_file(client: Minio, bucket: str, local_path: str, object_name: str) -> str:
    """Загрузка файла в S3/MinIO с подробным логированием.

    Возвращает object_name при успехе, пробрасывает исключение при ошибке.
    """
    logger = logging.getLogger(__name__)
    try:
        size = None
        try:
            size = os.path.getsize(local_path)
        except Exception:
            # размер не критичен для загрузки
            pass

        logger.info(
            "[S3][UPLOAD][START] bucket=%s key=%s path=%s size=%s",
            bucket,
            object_name,
            local_path,
            size,
        )
        client.fput_object(bucket, object_name, local_path)
        logger.info(
            "[S3][UPLOAD][DONE] bucket=%s key=%s path=%s", bucket, object_name, local_path
        )
        return object_name
    except S3Error as e:
        logger.error(
            "[S3][UPLOAD][ERROR] bucket=%s key=%s path=%s code=%s message=%s",
            bucket,
            object_name,
            local_path,
            getattr(e, "code", None),
            str(e),
        )
        raise
    except Exception as e:
        logger.exception(
            "[S3][UPLOAD][ERROR] bucket=%s key=%s path=%s unexpected error",
            bucket,
            object_name,
            local_path,
        )
        raise


def get_presigned_url(client: Minio, bucket: str, object_name: str, expires: dt.timedelta = dt.timedelta(hours=1)) -> str:
    seconds = int(expires.total_seconds())
    # Ограничение MinIO/S3: максимум 7 дней; оставим как есть
    return client.presigned_get_object(bucket, object_name, expires=seconds)


def build_object_url(cfg: MinioConfig, bucket: str, object_name: str) -> str:
    """Строит HTTP(S) URL вида https://host/bucket/object.

    Использует internal_url из конфига (если у клиента включён public access,
    то объекты будут доступны по этому адресу).
    """
    parsed = urlparse(cfg.internal_url)
    scheme = parsed.scheme or "http"
    host = parsed.netloc or parsed.path
    return f"{scheme}://{host}/{bucket}/{object_name}"


__all__ = [
    "_make_client",
    "ensure_bucket",
    "upload_file",
    "get_presigned_url",
    "build_object_url",
]
