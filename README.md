# Working Scheduler — модерация объявлений (текст + изображения)

Проект выполняет пакетную модерацию объявлений:
- текст — проверяется простыми правилами (см. `src/text_moderator`),
- изображения — закрываются области с номерными знаками с помощью модели YOLO (см. `src/image_moderator`).

Результаты сохраняются в PostgreSQL, а обработанные изображения выгружаются в MinIO.


## Требования
- Windows 10/11 или Linux/macOS
- Python 3.10+ (рекомендуется 3.10–3.11)
- Доступ к PostgreSQL
- Доступ к MinIO (или совместимому S3)


## Установка
1. Клонировать репозиторий.
2. Создать виртуальное окружение и установить зависимости.

Windows (PowerShell):
```
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -U pip
pip install -r requirements.txt
```

Linux/macOS:
```
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt
```


## Настройка окружения
Переменные окружения читаются из файлов `src/.env` и `src/.env.local` (локальный файл имеет приоритет). Создайте `src/.env.local` по примеру ниже:

```
# PostgreSQL
DB_HOST=127.0.0.1
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=postgres
DB_NAME=moderation

# MinIO
MINIO_INTERNAL_URL=http://127.0.0.1:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_SYSTEM_BUCKET=system
MINIO_CLIENT_BUCKET=client
MINIO_CLIENT_PUBLIC_ACCESS=true

# Лимит пачки объявлений за запуск
BATCH_LIMIT=50

# Очищать папку вывода при старте (output)
CLEAN_OUTPUT_ON_START=false

# Коммитить результаты модерации в таблицу объявлений
COMMIT_RESULTS=false
```

Пояснения:
- если какой‑то параметр не задан, приложение завершится с ошибкой с указанием отсутствующей переменной;
- `MINIO_CLIENT_PUBLIC_ACCESS=true` включит публичный доступ (read-only) для объекта в бакете клиента;
- таблицы в БД будут созданы автоматически при первом запуске (`init_db`).
 - `CLEAN_OUTPUT_ON_START=true` — при каждом запуске будет полностью удаляться папка `OUTPUT_FOLDER` (см. `src/ad_moderator.py`) и создаваться заново. Полезно для чистых прогонов; по умолчанию — `false`.
 - `COMMIT_RESULTS=true` — после завершения модерации каждого объявления (только если текущий статус `PAID`) приложение:
   - установит статус `REJECTED`, если текстовая модерация выявила нарушения (есть детекции с `type="text"`);
   - иначе установит статус `MODERATED`.
   В обоих случаях поле `moderated_at` будет установлено в `NOW()`. По умолчанию — `false`.


## Модель и пути к файлам
В `src/ad_moderator.py` заданы абсолютные пути по умолчанию:

```
OUTPUT_FOLDER = C:\Code\Python\working_sheduler\src\image_moderator\output
MODEL_PATH   = C:\Code\Python\working_sheduler\src\image_moderator\models\license-plate-finetune-v1l.onnx
```

При необходимости измените их под свою среду (на Windows — экранируйте обратные слэши или используйте raw-строки в коде).


## Запуск проекта
Из корня репозитория:

Windows (PowerShell):
```
.\.venv\Scripts\Activate.ps1
python -m src.ad_moderator
```

Linux/macOS:
```
source .venv/bin/activate
python -m src.ad_moderator
```

Альтернативно можно запустить файл напрямую:
```
python src\ad_moderator.py    # Windows
python src/ad_moderator.py    # Linux/macOS
```

Что происходит при запуске:
1. Загружается конфигурация из `.env`/`.env.local`.
2. Инициализируется PostgreSQL (создаются необходимые таблицы) и клиент MinIO.
3. Из БД выбирается пачка платных объявлений (`BATCH_LIMIT`).
4. Выполняется модерация текста и изображений.
5. Результат сохраняется в БД; покрытые изображения загружаются в MinIO.
6. Для отладки локально сохраняется `verdict_<ad_id>.json` в `OUTPUT_FOLDER`.


## Структура проекта (основное)
- `src/ad_moderator.py` — входная точка пакетной модерации.
- `src/config.py` — загрузка конфигурации из переменных окружения.
- `src/db.py` — работа с PostgreSQL (инициализация, выборки, сохранение результатов).
- `src/storage.py` — вспомогательные функции для MinIO.
- `src/utils.py` — утилиты, включая загрузку файлов по URL.
- `src/text_moderator/` — правила/логика текстовой модерации.
- `src/image_moderator/` — модерация изображений (YOLO, OpenCV), модели и примеры.


## Частые вопросы
- Ошибка про отсутствующие переменные окружения — проверьте `src/.env.local` по примеру выше.
- Нет соединения с PostgreSQL — проверьте `DB_HOST`, `DB_PORT`, доступы пользователя и сетевые правила.
- Нет доступа к MinIO — проверьте `MINIO_*` и что бакеты существуют; утилита сама создаст их при первом запуске.
- Модель не найдена — проверьте `MODEL_PATH` и наличие файла `.onnx`.


## Разработка
- Форматирование/линтинг не навязаны; придерживайтесь стиля существующего кода.
- Dependencies — см. `requirements.txt`.


## Лицензия
Если лицензия требуется, добавьте соответствующий раздел и файл `LICENSE`.