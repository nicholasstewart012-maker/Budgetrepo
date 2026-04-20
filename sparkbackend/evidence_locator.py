from __future__ import annotations

import re
from dataclasses import dataclass, asdict
from typing import Any, Iterable

COMMON_TOKENS = {
    "the", "and", "for", "are", "that", "with", "this", "from", "have", "has", "you", "your",
    "was", "were", "will", "shall", "must", "may", "can", "not", "all", "any", "policy", "section",
    "document", "employee", "employees", "company", "page", "pages", "source", "answer", "question",
}


@dataclass(slots=True)
class EvidenceCandidate:
    text: str
    source_type: str
    source_id: str = ""
    page_number: int | None = None
    block_id: str = ""
    row_number: int | None = None
    column_names: list[str] | None = None
    section_title: str = ""
    base_score: float = 0.0


@dataclass(slots=True)
class EvidenceMatch:
    anchor_text: str
    context_text: str
    match_type: str
    confidence: float
    page_number: int | None = None
    block_id: str = ""
    row_number: int | None = None
    column_names: list[str] | None = None
    section_title: str = ""
    source_type: str = ""
    source_id: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class EvidenceLocatorInput:
    question: str
    answer: str
    chunk_text: str = ""
    snippet: str = ""
    page_number: int | None = None
    chunk_id: str = ""
    section_title: str = ""
    block_id: str = ""
    document_type: str = "text"
    structured_blocks: list[dict[str, Any]] | None = None
    sheet_name: str = ""
    headers: list[str] | None = None
    row_number: int | None = None
    row_cells: list[dict[str, Any]] | None = None


class EvidenceLocator:
    def locate(self, payload: EvidenceLocatorInput) -> EvidenceMatch | None:
        doc_type = (payload.document_type or "text").lower()
        if doc_type in {"xlsx", "xlsm", "csv", "spreadsheet"}:
            candidates = self._spreadsheet_candidates(payload)
        else:
            candidates = self._text_candidates(payload)

        if not candidates:
            return None

        scored = [self._score_candidate(payload.question, payload.answer, candidate) for candidate in candidates]
        scored.sort(key=lambda item: item[0], reverse=True)
        best_score, best = scored[0]
        context_text = self._build_context(payload, best)

        return EvidenceMatch(
            anchor_text=best.text.strip(),
            context_text=context_text.strip() or best.text.strip(),
            match_type=best.source_type,
            confidence=self._confidence(best_score),
            page_number=best.page_number,
            block_id=best.block_id,
            row_number=best.row_number,
            column_names=best.column_names,
            section_title=best.section_title,
            source_type=best.source_type,
            source_id=best.source_id,
        )

    def _text_candidates(self, payload: EvidenceLocatorInput) -> list[EvidenceCandidate]:
        candidates: list[EvidenceCandidate] = []

        blocks = payload.structured_blocks or []
        if blocks:
            for idx, block in enumerate(blocks):
                block_text = str(block.get("text") or "").strip()
                if not block_text:
                    continue
                block_id = str(block.get("blockId") or block.get("id") or f"block_{idx}")
                section_title = str(block.get("sectionTitle") or payload.section_title or "")
                page_number = block.get("pageNumber", payload.page_number)
                sentences = _split_sentences(block_text)
                if not sentences:
                    sentences = [block_text]
                for s_idx, sentence in enumerate(sentences):
                    candidates.append(EvidenceCandidate(
                        text=sentence,
                        source_type="sentence",
                        source_id=f"{block_id}:s{s_idx}",
                        page_number=page_number,
                        block_id=block_id,
                        section_title=section_title,
                        base_score=0.35,
                    ))
                candidates.append(EvidenceCandidate(
                    text=block_text,
                    source_type="block",
                    source_id=block_id,
                    page_number=page_number,
                    block_id=block_id,
                    section_title=section_title,
                    base_score=0.2,
                ))
        else:
            text = "\n\n".join(part for part in [payload.snippet, payload.chunk_text] if part).strip()
            if not text:
                return []
            paragraphs = [p.strip() for p in re.split(r"\n\s*\n+", text) if p.strip()]
            for p_idx, paragraph in enumerate(paragraphs or [text]):
                sentences = _split_sentences(paragraph)
                if not sentences:
                    sentences = [paragraph]
                for s_idx, sentence in enumerate(sentences):
                    candidates.append(EvidenceCandidate(
                        text=sentence,
                        source_type="sentence",
                        source_id=f"chunk:s{p_idx}_{s_idx}",
                        page_number=payload.page_number,
                        block_id=payload.block_id,
                        section_title=payload.section_title,
                        base_score=0.35,
                    ))
                candidates.append(EvidenceCandidate(
                    text=paragraph,
                    source_type="block",
                    source_id=f"chunk:p{p_idx}",
                    page_number=payload.page_number,
                    block_id=payload.block_id,
                    section_title=payload.section_title,
                    base_score=0.2,
                ))

        return _dedupe_candidates(candidates)

    def _spreadsheet_candidates(self, payload: EvidenceLocatorInput) -> list[EvidenceCandidate]:
        cells = payload.row_cells or []
        if not cells:
            return []

        headers = payload.headers or []
        row_parts: list[str] = []
        column_names: list[str] = []
        candidates: list[EvidenceCandidate] = []

        for idx, cell in enumerate(cells):
            header = str(cell.get("header") or (headers[idx] if idx < len(headers) else "")).strip()
            value = str(cell.get("value") or "").strip()
            if not value:
                continue
            column_names.append(header or f"Column {idx + 1}")
            combined = f"{header}: {value}".strip(": ")
            row_parts.append(combined)
            candidates.append(EvidenceCandidate(
                text=combined,
                source_type="cell",
                source_id=f"cell:{idx}",
                row_number=payload.row_number,
                column_names=[header] if header else None,
                section_title=payload.sheet_name,
                base_score=0.4,
            ))

        if row_parts:
            candidates.append(EvidenceCandidate(
                text=" | ".join(row_parts),
                source_type="row",
                source_id=f"row:{payload.row_number or 0}",
                row_number=payload.row_number,
                column_names=column_names,
                section_title=payload.sheet_name,
                base_score=0.25,
            ))

        return candidates

    def _score_candidate(self, question: str, answer: str, candidate: EvidenceCandidate) -> tuple[float, EvidenceCandidate]:
        score = candidate.base_score
        cand_norm = _norm(candidate.text)
        q_norm = _norm(question)
        a_norm = _norm(answer)

        score += _token_overlap(candidate.text, answer) * 7.5
        score += _token_overlap(candidate.text, question) * 3.0

        if a_norm and a_norm in cand_norm:
            score += 8.0
        if q_norm and q_norm in cand_norm:
            score += 2.0

        answer_sentences = [s for s in _split_sentences(answer) if len(_norm(s)) >= 20]
        for sentence in answer_sentences:
            if _norm(sentence) in cand_norm:
                score += 5.0

        answer_numbers = _extract_numbers(answer)
        cand_numbers = _extract_numbers(candidate.text)
        if answer_numbers:
            score += len(answer_numbers & cand_numbers) * 4.0
            score -= len(answer_numbers - cand_numbers) * 1.5

        if candidate.section_title:
            score += _token_overlap(candidate.section_title, question) * 1.25

        if candidate.source_type == "sentence":
            score += 0.6
        if candidate.source_type == "cell":
            score += 0.5

        return score, candidate

    def _build_context(self, payload: EvidenceLocatorInput, best: EvidenceCandidate) -> str:
        if best.source_type in {"row", "cell"}:
            row_cells = payload.row_cells or []
            pairs = []
            for idx, cell in enumerate(row_cells):
                header = str(cell.get("header") or "").strip()
                value = str(cell.get("value") or "").strip()
                if value:
                    pairs.append(f"{header}: {value}".strip(": "))
            return " | ".join(pairs)

        blocks = payload.structured_blocks or []
        if best.block_id and blocks:
            for block in blocks:
                block_id = str(block.get("blockId") or block.get("id") or "")
                if block_id == best.block_id:
                    text = str(block.get("text") or "").strip()
                    return _context_window(text, best.text)

        text = payload.chunk_text or payload.snippet or best.text
        return _context_window(text, best.text)

    def _confidence(self, score: float) -> float:
        value = max(0.0, min(1.0, score / 12.0))
        return round(value, 4)


def locate_evidence(payload: EvidenceLocatorInput | dict[str, Any]) -> dict[str, Any] | None:
    locator = EvidenceLocator()
    if isinstance(payload, dict):
        payload = EvidenceLocatorInput(**payload)
    result = locator.locate(payload)
    return result.to_dict() if result else None


def _dedupe_candidates(candidates: Iterable[EvidenceCandidate]) -> list[EvidenceCandidate]:
    out: list[EvidenceCandidate] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = _norm(candidate.text)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(candidate)
    return out


def _split_sentences(text: str) -> list[str]:
    cleaned = re.sub(r"\s+", " ", str(text or "")).strip()
    if not cleaned:
        return []
    parts = re.split(r"(?<=[.!?])\s+", cleaned)
    return [part.strip() for part in parts if part.strip()]


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip().lower()


def _tokenize(text: str) -> list[str]:
    return re.findall(r"\b[a-z0-9][a-z0-9'-]*\b", _norm(text))


def _token_overlap(a: str, b: str) -> float:
    a_tokens = {t for t in _tokenize(a) if len(t) > 2 and t not in COMMON_TOKENS}
    b_tokens = {t for t in _tokenize(b) if len(t) > 2 and t not in COMMON_TOKENS}
    if not a_tokens or not b_tokens:
        return 0.0
    inter = len(a_tokens & b_tokens)
    union = len(a_tokens | b_tokens)
    return inter / union if union else 0.0


def _extract_numbers(text: str) -> set[str]:
    return set(re.findall(r"\b\d+(?:\.\d+)?\b", str(text or "")))


def _context_window(text: str, anchor: str, radius: int = 1) -> str:
    sentences = _split_sentences(text)
    if not sentences:
        return str(anchor or text or "").strip()

    anchor_norm = _norm(anchor)
    best_idx = 0
    best_score = -1.0
    for idx, sentence in enumerate(sentences):
        score = _token_overlap(sentence, anchor)
        if anchor_norm and anchor_norm in _norm(sentence):
            score += 2.0
        if score > best_score:
            best_idx = idx
            best_score = score

    start = max(0, best_idx - radius)
    end = min(len(sentences), best_idx + radius + 1)
    return " ".join(sentences[start:end]).strip()
