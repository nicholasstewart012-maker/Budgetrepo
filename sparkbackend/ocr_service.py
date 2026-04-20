from __future__ import annotations

import io
import os
import re
import shutil
from functools import lru_cache
from pathlib import Path
from typing import Any

import fitz
from PIL import Image

OCR_ENGINE = os.getenv("OCR_ENGINE", "tesseract").strip().lower()
TESSERACT_CMD = os.getenv("TESSERACT_CMD", "").strip()
ENABLE_TROCR_FALLBACK = os.getenv("ENABLE_TROCR_FALLBACK", "false").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
ENABLE_VISION_PDF_FALLBACK = os.getenv("ENABLE_VISION_PDF_FALLBACK", "false").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

_tesseract_warning_emitted = False
_tesseract_resolved_path: str | None = None

def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def count_meaningful_chars(text: str) -> int:
    cleaned = normalize_text(text)
    return len(re.sub(r"[^A-Za-z0-9]", "", cleaned))


def count_meaningful_words(text: str) -> int:
    return len(re.findall(r"\b[\w'-]+\b", text or ""))


def has_meaningful_text(text: str) -> bool:
    return count_meaningful_chars(text) >= 40 and count_meaningful_words(text) >= 8


@lru_cache(maxsize=1)
def _import_pytesseract() -> Any | None:
    try:
        import pytesseract
        return pytesseract
    except Exception:
        return None


def _candidate_tesseract_paths() -> list[str]:
    candidates: list[str] = []

    if TESSERACT_CMD:
        candidates.append(TESSERACT_CMD)

    which_path = shutil.which("tesseract")
    if which_path:
        candidates.append(which_path)

    candidates.extend([
        r"C:\Program Files\Tesseract-OCR\tesseract.exe",
        r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
    ])

    unique: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        normalized = str(Path(candidate))
        if normalized in seen:
            continue
        seen.add(normalized)
        unique.append(normalized)
    return unique


def _resolve_tesseract_cmd() -> str | None:
    global _tesseract_resolved_path
    if _tesseract_resolved_path is not None:
        return _tesseract_resolved_path

    pytesseract = _import_pytesseract()
    if pytesseract is None:
        _tesseract_resolved_path = None
        return None

    for candidate in _candidate_tesseract_paths():
        if not Path(candidate).exists():
            continue
        try:
            pytesseract.pytesseract.tesseract_cmd = candidate
            pytesseract.get_tesseract_version()
            _tesseract_resolved_path = candidate
            print(f"[Spark OCR] Using Tesseract at: {candidate}")
            return candidate
        except Exception:
            continue

    _tesseract_resolved_path = None
    return None


def _ensure_tesseract_available() -> bool:
    global _tesseract_warning_emitted, _tesseract_resolved_path
    if _resolve_tesseract_cmd() is not None:
        return True

    pytesseract = _import_pytesseract()
    if pytesseract is None:
        if not _tesseract_warning_emitted:
            print("[Spark OCR] Tesseract unavailable. Install Tesseract OCR or set TESSERACT_CMD.")
            _tesseract_warning_emitted = True
        return False

    try:
        if TESSERACT_CMD:
            pytesseract.pytesseract.tesseract_cmd = TESSERACT_CMD
            pytesseract.get_tesseract_version()
            _tesseract_resolved_path = TESSERACT_CMD
            print(f"[Spark OCR] Using Tesseract at: {TESSERACT_CMD}")
            return True
    except Exception:
        pass

    if not _tesseract_warning_emitted:
        print("[Spark OCR] Tesseract unavailable. Install Tesseract OCR or set TESSERACT_CMD.")
        _tesseract_warning_emitted = True
    return False


@lru_cache(maxsize=1)
def _load_trocr() -> tuple[Any, Any] | None:
    if not ENABLE_TROCR_FALLBACK:
        return None

    try:
        import torch
        from transformers import TrOCRProcessor, VisionEncoderDecoderModel
    except Exception as exc:
        print(f"[Spark OCR] TrOCR fallback unavailable: {exc}")
        return None

    model_name = os.getenv("OCR_MODEL_NAME", "microsoft/trocr-small-printed")
    try:
        processor = TrOCRProcessor.from_pretrained(model_name)
        model = VisionEncoderDecoderModel.from_pretrained(model_name)
        model.eval()
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        model.to(device)
        return processor, model
    except Exception as exc:
        print(f"[Spark OCR] Failed to load OCR model {model_name}: {exc}")
        return None


def render_pdf_page_to_image(pdf_path: str | Path, page_index: int, dpi: int = 200) -> Image.Image:
    doc = fitz.open(str(pdf_path))
    try:
        page = doc.load_page(int(page_index))
        zoom = dpi / 72.0
        matrix = fitz.Matrix(zoom, zoom)
        pix = page.get_pixmap(matrix=matrix, alpha=False)
        image = Image.open(io.BytesIO(pix.tobytes("png")))
        return image.convert("RGB")
    finally:
        doc.close()


def _ocr_with_tesseract(image: Image.Image) -> dict[str, Any]:
    pytesseract = _import_pytesseract()
    if pytesseract is None:
        return {
            "text": "",
            "method": "ocr_failed",
            "engine": "tesseract",
            "confidence": None,
            "error": "pytesseract not importable",
            "charCount": 0,
            "wordCount": 0,
        }

    try:
        text = normalize_text(pytesseract.image_to_string(image, config="--psm 6"))
        confidence = None
        try:
            data = pytesseract.image_to_data(image, output_type=pytesseract.Output.DICT, config="--psm 6")
            values: list[float] = []
            for raw in data.get("conf", []) or []:
                try:
                    value = float(raw)
                except Exception:
                    continue
                if value >= 0:
                    values.append(value)
            if values:
                confidence = round(sum(values) / len(values) / 100.0, 4)
        except Exception:
            confidence = None

        return {
            "text": text,
            "method": "ocr",
            "engine": "tesseract",
            "confidence": confidence,
            "error": None,
            "charCount": count_meaningful_chars(text),
            "wordCount": count_meaningful_words(text),
        }
    except Exception as exc:
        return {
            "text": "",
            "method": "ocr_failed",
            "engine": "tesseract",
            "confidence": None,
            "error": str(exc),
            "charCount": 0,
            "wordCount": 0,
        }


def _ocr_with_trocr(image: Image.Image) -> dict[str, Any]:
    cached = _load_trocr()
    if cached is None:
        return {
            "text": "",
            "method": "ocr_failed",
            "engine": "trocr",
            "confidence": None,
            "error": "TrOCR fallback disabled or unavailable",
            "charCount": 0,
            "wordCount": 0,
        }

    processor, model = cached

    try:
        import torch

        inputs = processor(images=image, return_tensors="pt")
        device = next(model.parameters()).device
        pixel_values = inputs.pixel_values.to(device)

        with torch.no_grad():
            generated = model.generate(
                pixel_values,
                max_new_tokens=128,
                return_dict_in_generate=True,
                output_scores=True,
            )

        text = normalize_text(processor.batch_decode(generated.sequences, skip_special_tokens=True)[0])
        confidence: float | None = None

        scores = getattr(generated, "scores", None) or []
        token_ids = generated.sequences[:, 1:]
        token_probs: list[float] = []
        for step_scores, token_id in zip(scores, token_ids[0]):
            step_probs = torch.softmax(step_scores[0], dim=-1)
            token_prob = float(step_probs[int(token_id)].item())
            token_probs.append(token_prob)
        if token_probs:
            confidence = float(sum(token_probs) / len(token_probs))

        return {
            "text": text,
            "method": "ocr",
            "engine": "trocr",
            "confidence": round(confidence, 4) if confidence is not None else None,
            "error": None,
            "charCount": count_meaningful_chars(text),
            "wordCount": count_meaningful_words(text),
        }
    except Exception as exc:
        return {
            "text": "",
            "method": "ocr_failed",
            "engine": "trocr",
            "confidence": None,
            "error": str(exc),
            "charCount": 0,
            "wordCount": 0,
        }


def _ocr_failed_message() -> dict[str, Any]:
    return {
        "text": "",
        "method": "ocr_failed",
        "engine": OCR_ENGINE or "tesseract",
        "confidence": None,
        "error": "OCR unavailable",
        "charCount": 0,
        "wordCount": 0,
    }


def ocr_image(image: Image.Image) -> dict[str, Any]:
    if OCR_ENGINE != "tesseract":
        if ENABLE_TROCR_FALLBACK:
            return _ocr_with_trocr(image)
        return _ocr_failed_message()

    if not _ensure_tesseract_available():
        print("[Spark OCR] Tesseract unavailable. Skipping OCR fallback.")
        return _ocr_failed_message()

    return _ocr_with_tesseract(image)


def extract_pdf_page_text(pdf_path: str | Path, page_number: int) -> dict[str, Any]:
    try:
        image = render_pdf_page_to_image(pdf_path, max(0, int(page_number) - 1), dpi=200)
        result = ocr_image(image)
        result["text"] = normalize_text(result.get("text", ""))
        result["charCount"] = count_meaningful_chars(result["text"])
        result["wordCount"] = count_meaningful_words(result["text"])
        return result
    except Exception as exc:
        return {
            "text": "",
            "method": "ocr_failed",
            "engine": OCR_ENGINE or "tesseract",
            "confidence": None,
            "error": str(exc),
            "charCount": 0,
            "wordCount": 0,
        }
