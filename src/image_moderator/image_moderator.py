import os
import cv2
from ultralytics import YOLO
import numpy as np
import cv2

import os
import cv2
from ultralytics import YOLO


def moderate_images(image_paths, model_path, output_dir, ad_id):
    model = YOLO(model_path)
    detections = []

    for image_path in image_paths:
        image = cv2.imread(image_path)
        if image is None:
            continue

        results = model.predict(source=image_path)
        annotated = image.copy()

        image_detections_count = 0

        for result in results:
            for box in result.boxes.xyxy:
                x1, y1, x2, y2 = map(int, box)

                # 1️⃣ Красивая плашка
                draw_rounded_box(
                    annotated,
                    x1, y1, x2, y2,
                    radius=12,
                    color=(255, 255, 255),
                    alpha=1
                )

                # 2️⃣ Текст
                text = "autoboyarin.ru"
                font = cv2.FONT_HERSHEY_SIMPLEX
                thickness = 2
                font_scale = 1.0

                max_width = max(x2 - x1 - 16, 20)

                while font_scale > 0.4:
                    (tw, th), _ = cv2.getTextSize(text, font, font_scale, thickness)
                    if tw <= max_width:
                        break
                    font_scale -= 0.1

                text_x = x1 + (x2 - x1 - tw) // 2
                text_y = y1 + (y2 - y1 + th) // 2

                cv2.putText(
                    annotated,
                    text,
                    (text_x, text_y),
                    font,
                    font_scale,
                    (20, 20, 20),  # мягкий чёрный
                    thickness,
                    cv2.LINE_AA
                )

                detections.append({
                    "type": "image",
                    "category": "license_plate",
                    "image": image_path
                })

                image_detections_count += 1

        # ---------- сохранение ----------
        if image_detections_count > 0:
            target_dir = os.path.join(output_dir, str(ad_id))
            os.makedirs(target_dir, exist_ok=True)

            out_path = os.path.join(
                target_dir,
                "covered_" + os.path.basename(image_path)
            )

            cv2.imwrite(out_path, annotated)

            # Проставляем output_path всем детекциям этого изображения
            for i in range(image_detections_count):
                detections[-(i + 1)]["output_path"] = out_path

    return detections

def draw_rounded_box(img, x1, y1, x2, y2, radius=10, color=(255, 255, 255), alpha=0.85):
    overlay = img.copy()

    w = x2 - x1
    h = y2 - y1
    radius = min(radius, w // 2, h // 2)

    mask = np.zeros((h, w, 3), dtype=np.uint8)

    # центральные прямоугольники
    cv2.rectangle(mask, (radius, 0), (w - radius, h), color, -1)
    cv2.rectangle(mask, (0, radius), (w, h - radius), color, -1)

    # углы
    cv2.circle(mask, (radius, radius), radius, color, -1)
    cv2.circle(mask, (w - radius, radius), radius, color, -1)
    cv2.circle(mask, (radius, h - radius), radius, color, -1)
    cv2.circle(mask, (w - radius, h - radius), radius, color, -1)

    roi = overlay[y1:y2, x1:x2]
    cv2.addWeighted(mask, alpha, roi, 1 - alpha, 0, roi)

    img[y1:y2, x1:x2] = roi