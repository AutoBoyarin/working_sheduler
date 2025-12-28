import cv2
from ultralytics import YOLO
import os
import glob

# Параметры
INPUT_FOLDER = r"C:\Code\Python\autoboyarin_moderator\src\autoboyarin_moderator\example\1"
MODEL_PATH = r"C:\Code\Python\autoboyarin_moderator\src\models\license-plate-finetune-v1l.onnx"
OUTPUT_FOLDER = r"C:\Code\Python\autoboyarin_moderator\src\autoboyarin_moderator\output\covered_plates_centered"

# Создаем папку, если ее нет
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# Загружаем модель
print(f"Loading {MODEL_PATH} for ONNX Runtime inference...")
model = YOLO(MODEL_PATH)

# Получаем все изображения из папки
image_paths = glob.glob(os.path.join(INPUT_FOLDER, "*.*"))
image_paths = [p for p in image_paths if p.lower().endswith((".jpg", ".jpeg", ".png"))]

print(f"Found {len(image_paths)} images in folder: {INPUT_FOLDER}")

# Обрабатываем каждое изображение
for IMAGE_PATH in image_paths:
    image_name = os.path.basename(IMAGE_PATH)
    print(f"\n=== Обработка: {image_name} ===")

    image = cv2.imread(IMAGE_PATH)
    if image is None:
        print(f"Не удалось загрузить изображение: {IMAGE_PATH}")
        continue

    annotated_frame = image.copy()
    results = model.predict(source=IMAGE_PATH)

    plate_count = 0
    for result in results:
        boxes = result.boxes.xyxy  # координаты боксов

        for box in boxes:
            x1, y1, x2, y2 = map(int, box)

            # Закрываем номер белой плашкой
            cv2.rectangle(annotated_frame, (x1, y1), (x2, y2), (255, 255, 255), -1)

            # Определяем размер текста, чтобы он помещался в ширину номера
            text = "autoboyarin.ru"
            max_width = x2 - x1 - 10  # оставляем небольшой отступ
            font_scale = 1.0
            thickness = 2
            while True:
                (text_width, text_height), _ = cv2.getTextSize(text, cv2.FONT_HERSHEY_SIMPLEX, font_scale, thickness)
                if text_width <= max_width or font_scale <= 0.3:
                    break
                font_scale -= 0.1

            # Позиционируем текст по центру плашки
            text_x = x1 + (x2 - x1 - text_width) // 2
            text_y = y1 + (y2 - y1 + text_height) // 2

            cv2.putText(
                annotated_frame,
                text,
                (text_x, text_y),
                cv2.FONT_HERSHEY_SIMPLEX,
                font_scale,
                (0, 0, 0),
                thickness,
                cv2.LINE_AA
            )

            plate_count += 1

    output_path = os.path.join(OUTPUT_FOLDER, f"covered_{image_name}")
    cv2.imwrite(output_path, annotated_frame)
    print(f"Сохранено изображение с закрытыми номерами: {output_path}, всего {plate_count} номеров")
