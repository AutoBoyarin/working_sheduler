from __future__ import annotations

import datetime as dt
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
    client.fput_object(bucket, object_name, local_path)
    return object_name


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
