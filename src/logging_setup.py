from __future__ import annotations

import json
import logging
import os
import sys
from logging.handlers import RotatingFileHandler


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "time": self.formatTime(record, datefmt="%Y-%m-%d %H:%M:%S"),
            "level": record.levelname,
            "name": record.name,
            "message": record.getMessage(),
        }
        # Добавим основные поля, если есть
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def _make_formatter(fmt: str) -> logging.Formatter:
    if fmt == "json":
        return _JsonFormatter()
    # text
    return logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")


def setup_logging(cfg) -> None:
    """Инициализация логирования по конфигу.

    cfg: объект с полями level, format (text|json), to_file, file, max_bytes, backup_count
    """
    level = getattr(logging, str(getattr(cfg, "level", "INFO")).upper(), logging.INFO)
    fmt = str(getattr(cfg, "format", "text")).lower()
    to_file = bool(getattr(cfg, "to_file", False))
    file_path = str(getattr(cfg, "file", "logs/app.log"))
    max_bytes = int(getattr(cfg, "max_bytes", 5 * 1024 * 1024))
    backup_count = int(getattr(cfg, "backup_count", 3))

    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Удаляем старые хендлеры, чтобы не было дублирования
    for h in list(root_logger.handlers):
        root_logger.removeHandler(h)

    formatter = _make_formatter(fmt)

    # Консольный вывод всегда
    sh = logging.StreamHandler(stream=sys.stdout)
    sh.setLevel(level)
    sh.setFormatter(formatter)
    root_logger.addHandler(sh)

    # Файловый вывод опционально
    if to_file:
        # Создадим папку, если нужно
        try:
            os.makedirs(os.path.dirname(os.path.abspath(file_path)), exist_ok=True)
        except Exception:
            pass
        fh = RotatingFileHandler(file_path, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8")
        fh.setLevel(level)
        fh.setFormatter(formatter)
        root_logger.addHandler(fh)

    # Немного сведений при старте
    logging.getLogger(__name__).info(
        "[LOGGING][INIT] level=%s format=%s to_file=%s file=%s",
        getattr(cfg, "level", "INFO"),
        fmt,
        to_file,
        file_path,
    )
