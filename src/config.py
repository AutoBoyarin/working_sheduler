from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional


def _str_to_bool(val: Optional[str], default: bool = False) -> bool:
    if val is None:
        return default
    return str(val).strip().lower() in {"1", "true", "yes", "y", "on"}


def _load_env_file(path: str) -> None:
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip()
            # Не перезаписываем уже заданные переменные окружения
            os.environ.setdefault(k, v)


@dataclass
class DbConfig:
    host: str
    port: int
    user: str
    password: str
    name: str


@dataclass
class MinioConfig:
    internal_url: str
    access_key: str
    secret_key: str
    system_bucket: str
    client_bucket: str
    client_public_access: bool = False


@dataclass
class AppConfig:
    db: DbConfig
    minio: MinioConfig
    batch_limit: int = 50
    clean_output_on_start: bool = False
    commit_results: bool = False


def load_config() -> AppConfig:
    # Загружаем .env.local (приоритет) и потом .env
    root = os.path.dirname(os.path.abspath(__file__))
    env_local = os.path.join(root, ".env.local")
    env_common = os.path.join(root, ".env")
    _load_env_file(env_common)
    _load_env_file(env_local)

    # DB
    db_host = os.environ.get("DB_HOST")
    db_port = int(os.environ.get("DB_PORT", "5432"))
    db_user = os.environ.get("DB_USER")
    db_password = os.environ.get("DB_PASSWORD")
    db_name = os.environ.get("DB_NAME")

    for k, v in {
        "DB_HOST": db_host,
        "DB_USER": db_user,
        "DB_PASSWORD": db_password,
        "DB_NAME": db_name,
    }.items():
        if not v:
            raise RuntimeError(f"Missing required env var: {k}")

    db_cfg = DbConfig(
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_password,
        name=db_name,
    )

    # MinIO
    minio_internal_url = os.environ.get("MINIO_INTERNAL_URL")
    minio_access_key = os.environ.get("MINIO_ACCESS_KEY")
    minio_secret_key = os.environ.get("MINIO_SECRET_KEY")
    minio_system_bucket = os.environ.get("MINIO_SYSTEM_BUCKET")
    minio_client_bucket = os.environ.get("MINIO_CLIENT_BUCKET")
    minio_public = _str_to_bool(os.environ.get("MINIO_CLIENT_PUBLIC_ACCESS"), False)

    for k, v in {
        "MINIO_INTERNAL_URL": minio_internal_url,
        "MINIO_ACCESS_KEY": minio_access_key,
        "MINIO_SECRET_KEY": minio_secret_key,
        "MINIO_SYSTEM_BUCKET": minio_system_bucket,
        "MINIO_CLIENT_BUCKET": minio_client_bucket,
    }.items():
        if not v:
            raise RuntimeError(f"Missing required env var: {k}")

    minio_cfg = MinioConfig(
        internal_url=minio_internal_url,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        system_bucket=minio_system_bucket,
        client_bucket=minio_client_bucket,
        client_public_access=minio_public,
    )

    batch_limit = int(os.environ.get("BATCH_LIMIT", "50"))
    clean_output_on_start = _str_to_bool(os.environ.get("CLEAN_OUTPUT_ON_START"), False)
    commit_results = _str_to_bool(os.environ.get("COMMIT_RESULTS"), False)

    return AppConfig(
        db=db_cfg,
        minio=minio_cfg,
        batch_limit=batch_limit,
        clean_output_on_start=clean_output_on_start,
        commit_results=commit_results,
    )
