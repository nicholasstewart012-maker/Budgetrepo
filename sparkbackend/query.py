from __future__ import annotations
import os
import json
import copy
import asyncio
import hashlib
import time
import re
import tempfile
import traceback
from pathlib import Path
from datetime import datetime, timedelta
from functools import lru_cache
from collections import Counter
from uuid import uuid4
from typing import Any
from dotenv import load_dotenv
import chromadb
try:
    from spark_db import (
        get_active_source_paths,
        get_document_stats,
        get_recent_query_logs,
        log_query as db_log_query,
        search_chunks_fts,
        update_query_feedback as db_update_query_feedback,
    )
except ModuleNotFoundError:
    from backend.spark_db import (
        get_active_source_paths,
        get_document_stats,
        get_recent_query_logs,
        log_query as db_log_query,
        search_chunks_fts,
        update_query_feedback as db_update_query_feedback,
    )

try:
    from evidence_locator import locate_evidence
except ModuleNotFoundError:
    from backend.evidence_locator import locate_evidence

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env", override=True)
print(f"[Spark Live] query.__file__={__file__} cwd={os.getcwd()}")
CONVERSATIONS_FOLDER = Path(os.getenv("CONVERSATIONS_FOLDER", r"C:\Spark\conversations"))
CHROMA_FOLDER = Path(os.getenv("CHROMA_FOLDER", r"C:\Spark\chromadb"))
CHROMA_COLLECTION = os.getenv("CHROMA_COLLECTION", "spark_documents")
LLM_MODEL = os.getenv("LLM_MODEL", "llama3.1:8b")
LIMINAL_URL = os.getenv("LIMINAL_URL", "http://localhost:11434")
LIMINAL_TOKEN = os.getenv("LIMINAL_TOKEN", "your_token_here")
print(f"[Spark Query] Model: Answer={LLM_MODEL}")
TOP_K = 20
MAX_HISTORY_ENTRIES = 200
LIMINAL_RETRIES = 3
LIMINAL_RETRY_BASE = 0.5
MAX_CONTEXT_SOURCES = int(os.getenv("MAX_CONTEXT_SOURCES", "4"))
MAX_CONTEXT_CHUNKS = int(os.getenv("MAX_CONTEXT_CHUNKS", "6"))
MAX_CHUNKS_PER_SOURCE = int(os.getenv("MAX_CHUNKS_PER_SOURCE", "2"))
MIN_VECTOR_SCORE = float(os.getenv("MIN_VECTOR_SCORE", "0.45"))
MIN_BM25_SCORE = float(os.getenv("MIN_BM25_SCORE", "10.0"))
HISTORY_RETENTION_DAYS = int(os.getenv("HISTORY_RETENTION_DAYS", "90"))
QUERY_EMBED_CACHE_SIZE = int(os.getenv("QUERY_EMBED_CACHE_SIZE", "256"))
ADMIN_NOTES_FILE = Path(os.getenv("ADMIN_NOTES_FILE", r"C:\Spark\admin_notes.json"))
# fast path for UI-ish work
FAST_LIMINAL_TIMEOUT_SECONDS = int(os.getenv("FAST_LIMINAL_TIMEOUT_SECONDS", "3"))
FAST_LIMINAL_RETRIES = int(os.getenv("FAST_LIMINAL_RETRIES", "1"))
# Reranker Placeholders
RERANKER_ENABLED = os.getenv("RERANKER_ENABLED", "false").lower() == "true"
RERANKER_MODEL_PATH = os.getenv("RERANKER_MODEL_PATH", r"C:\Spark\reranker")
RERANKER_TOP_K = int(os.getenv("RERANKER_TOP_K", "40"))
RERANKER_FINAL_K = int(os.getenv("RERANKER_FINAL_K", "8"))
TOPIC_GUARD_MIN_COVERAGE = float(os.getenv("TOPIC_GUARD_MIN_COVERAGE", "0.5"))
TOPIC_GUARD_MAX_CHUNKS = int(os.getenv("TOPIC_GUARD_MAX_CHUNKS", "3"))
RESCUE_MIN_SCORE = float(os.getenv("RESCUE_MIN_SCORE", "15.0"))
# ── Text helpers ──
def _norm_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())
def _strip_inline_citations(s: str) -> str:
    if not s:
        return ""
    text = re.sub(r"\s*\[(?:\d+)\]", "", s)
    text = re.sub(r"\s+", " ", text).strip()
    return text
def _extract_numbers(s: str) -> set[str]:
    return set(re.findall(r"\b\d+\b", s or ""))
def _xlsx_headers(item: dict) -> list[str]:
    headers = item.get("headers")
    if isinstance(headers, str):
        try:
            parsed = json.loads(headers)
            if isinstance(parsed, list):
                return [str(value).strip() for value in parsed if str(value).strip()]
        except Exception:
            return [part.strip() for part in re.split(r"[;,|]+", headers) if part.strip()]
    if isinstance(headers, (list, tuple)):
        return [str(value).strip() for value in headers if str(value).strip()]
    return []
def _is_spreadsheet_chunk(item: dict) -> bool:
    return str(item.get("chunk_type") or "").startswith("spreadsheet_") or str(item.get("file_type") or "").lower() in {"xlsx", "xlsm", "csv"}
def _spreadsheet_question_intent(question: str) -> dict[str, bool]:
    q_norm = _norm_text(question)
    row_intent = any(term in q_norm for term in ("which row", "what row", "row ", "row#", "row number", "which item", "which entry", "what amount", "what status", "what value", "which loan", "which record"))
    broad_intent = any(term in q_norm for term in ("summary", "overview", "what does", "describe", "show me", "what is in", "list all"))
    return {"row_intent": row_intent, "broad_intent": broad_intent}
def _tokenize_for_search(text: str) -> list[str]:
    return re.findall(r"\b[a-z0-9]+\b", _norm_text(text))
def _split_sentences(text: str) -> list[str]:
    if not text:
        return []
    text = re.sub(r"\s+", " ", text).strip()
    parts = re.split(r'(?<=[.!?])\s+', text)
    return [p.strip() for p in parts if p.strip()]
def _token_overlap_score(a: str, b: str) -> float:
    a_tokens = set(_tokenize_for_search(a))
    b_tokens = set(_tokenize_for_search(b))
    if not a_tokens or not b_tokens:
        return 0.0
    inter = len(a_tokens & b_tokens)
    union = len(a_tokens | b_tokens)
    return inter / union if union else 0.0
def _sentence_match_score(sentence: str, question: str, answer: str) -> float:
    s_norm = _norm_text(sentence)
    q_norm = _norm_text(question)
    a_norm = _norm_text(answer)
    score = 0.0
    # Strongest signal: overlap with final answer text
    score += _token_overlap_score(sentence, answer) * 8.0
    # Secondary signal: overlap with question
    score += _token_overlap_score(sentence, question) * 3.0
    # Number matching matters a lot for policy facts
    ans_nums = _extract_numbers(answer)
    sent_nums = _extract_numbers(sentence)
    if ans_nums:
        matched = len(ans_nums & sent_nums)
        missed = len(ans_nums - sent_nums)
        score += matched * 5.0
        score -= missed * 3.0
    # Prefer sentences that explicitly contain quoted policy language from the answer
    for phrase in _split_sentences(answer):
        phrase = _norm_text(phrase)
        if len(phrase) >= 20 and phrase in s_norm:
            score += 10.0
    return score
def _make_chunk_id(source: str, chunk_index: int, text: str) -> str:
    base = f"{source}|{chunk_index}|{_norm_text(text)[:180]}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()[:16]
def _ms(start_time: float) -> int:
    return int((time.perf_counter() - start_time) * 1000)
def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))
def _normalize_quality_flags(flags: Any) -> list[str]:
    if not flags:
        return []
    if isinstance(flags, str):
        raw = flags.strip()
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return [str(item) for item in parsed if str(item).strip()]
        except Exception:
            pass
        return [part.strip() for part in re.split(r"[;,|]+", raw) if part.strip()]
    if isinstance(flags, (list, tuple, set)):
        return [str(item).strip() for item in flags if str(item).strip()]
    return [str(flags).strip()]
def _detect_question_intents(question: str) -> dict[str, bool]:
    q_norm = _norm_text(question)
    intent_keywords = {
        "purpose_intent": (
            "purpose",
            "objective",
            "objectives",
            "goal",
            "goals",
            "policy statement",
            "what is the purpose",
            "what's the purpose",
        ),
        "responsibility_intent": (
            "who is responsible",
            "responsibility",
            "responsibilities",
            "owner",
            "who should",
            "who must",
        ),
        "phase_intent": (
            "phases",
            "stages",
            "steps",
            "process",
        ),
        "access_control_intent": (
            "access control",
            "authentication",
            "authorization",
            "access rights",
        ),
        "procedure_intent": (
            "how do i",
            "how can i",
            "how should i",
            "what is the process",
            "what's the process",
            "what do i do",
            "where do i",
            "who do i contact",
            "how to",
            "request access",
            "report",
            "submit",
            "contact",
            "approval",
            "approve",
            "escalate",
            "escalation",
            "hotline",
        ),
        "reporting_intent": (
            "report",
            "hotline",
            "ethics",
            "compliance",
            "misconduct",
            "whistleblower",
            "whistle blower",
            "anonymous",
            "concern",
        ),
        "access_request_intent": (
            "request access",
            "get access",
            "system access",
            "application access",
            "shared drive",
            "network share",
            "shared folder",
            "file share",
        ),
        "access_removal_intent": (
            "remove access",
            "access removed",
            "revoke access",
            "termination",
            "offboarding",
            "employee leaves",
            "leaves the company",
            "separated employee",
        ),
        "travel_limit_intent": (
            "hotel limit",
            "business travel",
            "lodging",
            "reimbursement",
            "per night",
            "maximum",
            "max",
            "limit",
            "travel expenses",
        ),
    }
    return {
        intent: any(term in q_norm for term in keywords)
        for intent, keywords in intent_keywords.items()
    }
def _is_document_discovery_question(question: str) -> bool:
    q = _norm_text(question)
    if not q:
        return False

    doc_terms = r"(?:policy|policies|procedure|procedures|document|documents|standard|standards|guideline|guidelines|plan|plans)"
    action_terms = r"(?:talk(?:s)? about|cover(?:s)?|mention(?:s)?|address(?:es)?|say(?:s)?|discuss(?:es)?|handle(?:s)?|appl(?:y|ies) to|is about|are about|reference(?:s)?)"
    starter_terms = r"(?:what|which)"

    patterns = (
        rf"\b{starter_terms}\s+{doc_terms}\b",
        rf"\b{doc_terms}\s+{action_terms}\b",
        rf"\bwhere\s+(?:is|are|can|would|should)\b",
        rf"\bdo\s+we\s+have\s+(?:a|an|any)?\s*{doc_terms}\b",
        rf"\bis\s+there\s+(?:a|an|any)?\s*{doc_terms}\b",
        rf"\bare\s+there\s+(?:any)?\s*{doc_terms}\b",
    )
    if any(re.search(pattern, q) for pattern in patterns):
        return True

    has_doc_term = bool(re.search(rf"\b{doc_terms}\b", q))
    has_action_term = bool(re.search(rf"\b{action_terms}\b", q))
    return has_doc_term and has_action_term
def _is_procedural_question(question: str) -> bool:
    q = _norm_text(question)
    if not q:
        return False
    patterns = (
        "how do i",
        "how can i",
        "how should i",
        "what is the process",
        "what's the process",
        "what do i do",
        "where do i",
        "who do i contact",
        "how to",
        "request access",
        "report a concern",
        "report an ethics concern",
        "report a compliance concern",
        "report misconduct",
        "ethics concern",
        "compliance concern",
        "hotline",
        "whistleblower",
        "whistle blower",
        "anonymous",
        "submit a request",
        "escalate",
    )
    return any(pattern in q for pattern in patterns)
def _is_travel_limit_question(question: str) -> bool:
    q = _norm_text(question)
    if not q:
        return False
    travel_terms = ("hotel", "lodging", "business travel", "travel expenses", "expense", "reimbursement")
    limit_terms = ("limit", "maximum", "max", "cap", "per night", "rate", "allowance")
    return ("hotel limit" in q) or (any(term in q for term in travel_terms) and any(term in q for term in limit_terms))
def _expand_keyword_query(question: str) -> str:
    q_norm = _norm_text(question)
    extras: list[str] = []
    intents = _detect_question_intents(question)
    if intents.get("purpose_intent"):
        extras.extend(["purpose", "objective", "objectives", "goal", "goals", "policy statement", "executive summary"])
    if intents.get("responsibility_intent"):
        extras.extend(["responsibilities", "responsibility", "roles", "owner"])
    if intents.get("phase_intent"):
        extras.extend(["preparation", "detection", "analysis", "containment", "eradication", "recovery", "post-incident"])
    if intents.get("access_control_intent"):
        extras.extend(["access control", "authentication", "authorization", "least privilege", "need to know"])
    if intents.get("procedure_intent"):
        extras.extend(["process", "procedure", "request", "reporting", "escalation", "approval", "responsibilities", "hotline", "contact", "access"])
    if intents.get("reporting_intent"):
        extras.extend(["hotline", "whistleblower", "anonymous", "contact", "notify", "email", "phone", "reporting"])
    if intents.get("access_request_intent"):
        extras.extend(["access approval", "access rights", "authorization", "manager", "human resources", "information systems administrator"])
    if intents.get("access_removal_intent"):
        extras.extend(["terminate access", "remove access", "separated employee", "termination procedures", "human resources"])
    if _is_travel_limit_question(question):
        extras.extend(["hotel", "lodging", "reimbursement", "per night", "maximum", "limit", "travel expenses"])
    if not extras:
        return question
    expanded = f"{question} {' '.join(extras)}"
    if len(expanded) > len(question) + 240:
        return f"{question} {' '.join(extras[:6])}"
    return expanded
def _bm25_fallback_queries(question: str) -> list[str]:
    q = _norm_text(question)
    queries: list[str] = []
    intents = _detect_question_intents(question)
    if intents.get("access_request_intent"):
        queries.extend(["access approval", "access rights", "request access", "application access"])
    if intents.get("access_removal_intent"):
        queries.extend(["remove access", "termination procedures", "separated employee access", "access must immediately cease"])
    if intents.get("reporting_intent"):
        queries.extend([
            "whistleblower policy",
            "whistleblower anonymous",
            "security violations",
            "reporting security violations",
            "code ethics",
            "chief compliance officer",
            "email phone report",
        ])
    if _is_travel_limit_question(question):
        queries.extend(["hotel lodging", "hotel rate", "lodging policies", "travel lodging reimbursement"])
    deduped: list[str] = []
    seen: set[str] = set()
    for query in queries:
        if query not in seen:
            deduped.append(query)
            seen.add(query)
    return deduped
def _question_topic_terms(question: str) -> set[str]:
    q_norm = _norm_text(question)
    tokens = _tokenize_for_search(question)
    if not tokens:
        return set()
    stopwords = {
        "what", "who", "when", "where", "why", "how", "is", "are", "was", "were",
        "the", "a", "an", "of", "for", "to", "and", "or", "in", "on", "at", "by",
        "with", "from", "this", "that", "these", "those", "be", "do", "does", "did",
        "can", "could", "should", "would", "will", "may", "our", "your", "their",
        "there", "any", "about", "regarding", "policy", "procedure", "rule", "rules",
        "tell", "me", "please", "explain", "describe", "give", "show", "it", "its",
    }
    generic_numeric = {"many", "much", "long", "often", "days", "hours", "weeks", "months", "years", "number"}
    focus = {tok for tok in tokens if tok not in stopwords and len(tok) > 2}
    if q_norm.startswith("who "):
        focus -= {"person", "name", "title"}
    if q_norm.startswith("what is our policy on") or q_norm.startswith("what's our policy on"):
        focus -= {"what"}
    if q_norm.startswith("how "):
        focus -= generic_numeric
    return focus
def _topic_term_synonyms() -> dict[str, set[str]]:
    return {
        "ciso": {"chief information security officer", "information security officer"},
        "ceo": {"chief executive officer"},
        "cio": {"chief information officer"},
        "mfa": {"multi factor authentication", "multifactor authentication", "two factor authentication", "2fa"},
        "vpn": {"virtual private network"},
        "pto": {"paid time off", "vacation"},
        "chatbots": {"chatbot", "ai chatbot", "artificial intelligence chatbot", "generative ai"},
        "remote": {"telework", "work from home", "wfh"},
        "work": {"employment", "working"},
        "laptop": {"device", "computer", "equipment"},
        "cardholder": {"card holder", "cardholder data"},
        "access": {"permission", "permissions", "authorization", "provisioning", "approval", "access rights", "need to know"},
        "permissions": {"access", "authorization", "access rights", "provisioning"},
        "provisioning": {"access", "authorization", "permissions", "approval"},
        "authorization": {"access", "permission", "permissions", "approval"},
        "approval": {"authorization", "permission", "permissions", "approve"},
        "shared": {"shared drive", "shared folder", "file share", "network drive", "network share"},
        "drive": {"shared drive", "shared folder", "file share", "network drive", "network share"},
        "folder": {"shared folder", "file share", "network drive", "shared drive"},
        "application": {"system", "software", "program", "platform", "app"},
        "system": {"application", "software", "platform", "app"},
        "ethics": {"code of conduct", "hotline", "concern", "misconduct", "violation"},
        "compliance": {"regulatory", "reporting", "hotline", "concern", "violation"},
        "report": {"notify", "notification", "escalate", "submit", "disclose", "reporting"},
        "concern": {"issue", "complaint", "misconduct", "violation", "hotline"},
        "request": {"submit", "ticket", "approval", "provisioning"},
        "submit": {"request", "report", "notify"},
        "escalate": {"report", "notify", "escalation"},
        "hotline": {"ethics", "compliance", "reporting", "concern"},
    }
def _context_covers_topic(topic_terms: set[str], chunks: list[dict], min_coverage: float = TOPIC_GUARD_MIN_COVERAGE, max_chunks: int = TOPIC_GUARD_MAX_CHUNKS) -> tuple[bool, dict[str, Any]]:
    if not topic_terms:
        return True, {"matched_terms": [], "missing_terms": [], "coverage": 1.0}
    synonym_map = _topic_term_synonyms()
    text_parts: list[str] = []
    for chunk in chunks[:max_chunks]:
        text_parts.append(str(chunk.get("text", "")))
        text_parts.append(str(chunk.get("section_title", "")))
        text_parts.append(str(chunk.get("parent_section_title", "")))
        text_parts.append(str(chunk.get("source_title", "")))
    haystack = _norm_text(" ".join(part for part in text_parts if part))
    matched: set[str] = set()
    missing: set[str] = set()
    for term in topic_terms:
        expanded_terms = {term} | synonym_map.get(term, set())
        if any(_norm_text(candidate) in haystack for candidate in expanded_terms if candidate):
            matched.add(term)
        else:
            missing.add(term)
    coverage = len(matched) / max(len(topic_terms), 1)
    return coverage >= min_coverage, {
        "matched_terms": sorted(matched),
        "missing_terms": sorted(missing),
        "coverage": round(coverage, 4),
    }
def _chunk_noise_terms() -> set[str]:
    return {
        "table_of_contents",
        "disclaimer",
        "revision_history",
        "approval_metadata",
        "heading_only",
    }
def _chunk_ranking_adjustments(item: dict, intents: dict[str, bool], question: str) -> tuple[float, list[str]]:
    chunk_type = str(item.get("chunk_type") or "body")
    source_title = _norm_text(str(item.get("source_title") or item.get("source_name") or item.get("source") or ""))
    section_title = _norm_text(str(item.get("section_title") or ""))
    parent_section_title = _norm_text(str(item.get("parent_section_title") or ""))
    text = _norm_text(str(item.get("text") or ""))
    flags = set(_normalize_quality_flags(item.get("quality_flags")))
    score_delta = 0.0
    notes: list[str] = []
    if chunk_type in _chunk_noise_terms() or flags & _chunk_noise_terms():
        noise_penalty = {
            "table_of_contents": 1.35,
            "disclaimer": 1.6,
            "revision_history": 1.25,
            "approval_metadata": 1.15,
            "heading_only": 0.9,
        }.get(chunk_type, 0.75)
        score_delta -= noise_penalty
        notes.append(f"downrank:{chunk_type}")
    if chunk_type == "ocr":
        score_delta += 0.05
        notes.append("ocr_signal")
    if intents.get("purpose_intent"):
        if chunk_type in {"body", "ocr"}:
            score_delta += 0.28
            notes.append("purpose:body_boost")
        if any(term in section_title or term in parent_section_title for term in ("purpose", "objective", "objectives", "goal", "goals", "policy statement", "executive summary")):
            score_delta += 0.42
            notes.append("purpose:section_match")
        if chunk_type == "responsibility":
            score_delta -= 0.45
            notes.append("purpose:responsibility_penalty")
        if chunk_type == "table_of_contents":
            score_delta -= 0.9
        if chunk_type == "disclaimer":
            score_delta -= 1.2
    if intents.get("responsibility_intent"):
        if chunk_type == "responsibility":
            score_delta += 0.55
            notes.append("responsibility:chunk_match")
        if any(term in section_title or term in parent_section_title for term in ("responsibility", "responsibilities", "roles")):
            score_delta += 0.38
            notes.append("responsibility:section_match")
        if chunk_type in {"table_of_contents", "disclaimer", "revision_history"}:
            score_delta -= 0.35
    if intents.get("phase_intent"):
        phase_terms = ("phase", "phases", "stage", "stages", "process", "detection", "containment", "eradication", "recovery", "post-incident")
        if chunk_type in {"body", "ocr"} and any(term in section_title or term in parent_section_title or term in text for term in phase_terms):
            score_delta += 0.45
            notes.append("phase:section_match")
        if any(term in text for term in ("testing", "annual review", "exercise", "tabletop")) and "testing" not in _norm_text(question):
            score_delta -= 0.18
            notes.append("phase:testing_penalty")
    if intents.get("access_control_intent") and any(term in section_title or term in parent_section_title or term in text for term in ("access control", "authentication", "authorization", "least privilege", "need to know")):
        score_delta += 0.35
        notes.append("access_control:section_match")
    haystack = f"{section_title} {parent_section_title} {text}"
    if intents.get("procedure_intent"):
        procedure_terms = (
            "process",
            "procedure",
            "request",
            "reporting",
            "escalation",
            "approval",
            "responsibilities",
            "hotline",
            "contact",
            "access",
            "submit",
            "notify",
        )
        if chunk_type in {"body", "ocr", "responsibility"} and any(term in haystack for term in procedure_terms):
            score_delta += 0.42
            notes.append("procedure:workflow_match")
        if chunk_type == "responsibility":
            score_delta += 0.18
            notes.append("procedure:responsibility_boost")
        if any(term in section_title or term in parent_section_title for term in procedure_terms):
            score_delta += 0.28
            notes.append("procedure:section_match")
        if any(term in _norm_text(question) for term in ("ethics", "compliance", "concern", "report")):
            reporting_terms = (
                "whistleblower",
                "whistle blower",
                "code of ethics",
                "code of business conduct",
                "security violations",
                "reporting security violations",
                "suspected compromises",
                "suspected disclosure",
            )
            if any(term in haystack for term in reporting_terms):
                score_delta += 0.7
                notes.append("procedure:reporting_evidence")
            elif any(term in source_title for term in ("digital delivery", "travel and expense", "sox compliance")):
                score_delta -= 0.7
                notes.append("procedure:generic_compliance_penalty")
    if intents.get("reporting_intent"):
        question_norm = _norm_text(question)
        reporting_channel_terms = (
            "hotline",
            "report",
            "reporting",
            "whistleblower",
            "whistle blower",
            "anonymous",
            "contact",
            "notify",
            "incident response",
            "email",
            "e-mail",
            "phone",
            "voice mail",
            "voicemail",
        )
        if any(term in haystack for term in reporting_channel_terms):
            score_delta += 0.35
            notes.append("reporting:channel_match")
        if any(term in question_norm for term in ("hotline", "ethics", "anonymous", "misconduct")):
            if any(term in haystack for term in ("whistleblower", "whistle blower", "anonymous", "voice mail", "voicemail")):
                score_delta += 1.2
                notes.append("reporting:whistleblower_match")
            if "incident response" in source_title and not any(term in haystack for term in ("whistleblower", "anonymous")):
                score_delta -= 0.45
                notes.append("reporting:incident_response_penalty")
        if ("code of ethics" in haystack or "ethical conduct" in haystack) and not any(term in haystack for term in ("report", "hotline", "whistleblower", "anonymous", "contact")):
            score_delta -= 0.25
            notes.append("reporting:ethics_fluff_penalty")
        if any(term in source_title for term in ("accounts payable", "fixed assets", "travel and expense", "pci policy")):
            score_delta -= 0.25
            notes.append("reporting:domain_penalty")
    if intents.get("access_request_intent"):
        access_terms = ("access approval", "access rights", "authorization", "request", "manager", "human resources", "information systems administrator")
        if any(term in haystack for term in access_terms):
            score_delta += 0.3
            notes.append("access_request:evidence_match")
        if any(term in haystack for term in ("access approval", "user access program", "information systems administrator", "access rights administration")):
            score_delta += 0.8
            notes.append("access_request:approval_process_match")
        if any(term in haystack for term in ("bank control update", "general ledger interface", "general ledger solution")) and "general ledger" not in _norm_text(question):
            score_delta -= 1.0
            notes.append("access_request:gl_specific_penalty")
        if any(term in source_title for term in ("accounts payable", "accounting financial reporting", "pci policy", "fixed assets", "travel and expense", "technology change")):
            score_delta -= 1.2
            notes.append("access_request:domain_penalty")
    if intents.get("access_removal_intent"):
        removal_terms = ("termination", "termination procedures", "separated employee", "immediately cease", "remove access", "access changes")
        if any(term in haystack for term in removal_terms):
            score_delta += 0.35
            notes.append("access_removal:evidence_match")
        if any(term in source_title for term in ("accounts payable", "pci policy", "fixed assets", "travel and expense")):
            score_delta -= 1.2
            notes.append("access_removal:domain_penalty")
    if _is_travel_limit_question(question):
        travel_terms = (
            "hotel",
            "lodging",
            "reimbursement",
            "per night",
            "maximum",
            "limit",
            "travel expenses",
        )
        if any(term in haystack for term in travel_terms):
            score_delta += 0.46
            notes.append("travel_limit:content_match")
        if any(term in section_title or term in parent_section_title for term in travel_terms):
            score_delta += 0.24
            notes.append("travel_limit:section_match")
    sheet_name = _norm_text(str(item.get("sheet_name") or ""))
    range_ref = _norm_text(str(item.get("range_ref") or ""))
    headers = _xlsx_headers(item)
    spreadsheet_intent = _spreadsheet_question_intent(question)
    is_spreadsheet = _is_spreadsheet_chunk(item)
    if is_spreadsheet:
        score_delta += 0.12
        notes.append("spreadsheet")
        if chunk_type in {"spreadsheet_row_batch", "spreadsheet_key_value"}:
            score_delta += 0.45
            notes.append("spreadsheet:row_evidence")
        elif chunk_type == "spreadsheet_table_summary":
            score_delta += 0.08
            notes.append("spreadsheet:table_summary")
        elif chunk_type == "spreadsheet_sheet_summary":
            score_delta -= 0.08
            notes.append("spreadsheet:sheet_summary")
        question_norm = _norm_text(question)
        if sheet_name and sheet_name in question_norm:
            score_delta += 0.18
            notes.append("spreadsheet:sheet_name_match")
        if range_ref and range_ref in question_norm:
            score_delta += 0.18
            notes.append("spreadsheet:range_match")
        if headers and any(header and header in question_norm for header in headers):
            score_delta += 0.22
            notes.append("spreadsheet:header_match")
        if _extract_numbers(question) & _extract_numbers(text):
            score_delta += 0.14
            notes.append("spreadsheet:value_match")
        if spreadsheet_intent["row_intent"]:
            if chunk_type in {"spreadsheet_row_batch", "spreadsheet_key_value"}:
                score_delta += 0.24
                notes.append("spreadsheet:row_intent")
            elif chunk_type == "spreadsheet_sheet_summary":
                score_delta -= 0.2
                notes.append("spreadsheet:downrank_summary")
            elif chunk_type == "spreadsheet_table_summary":
                score_delta -= 0.08
                notes.append("spreadsheet:downrank_table_summary")
        if spreadsheet_intent["broad_intent"] and chunk_type == "spreadsheet_sheet_summary":
            score_delta += 0.24
            notes.append("spreadsheet:broad_summary")
    return score_delta, notes
_chroma_client = None
_chroma_collection = None
_history_lock = asyncio.Lock()
_notes_lock = asyncio.Lock()
try:
    from embedding_config import call_embedding_api, get_active_embedding_config
except ImportError:
    from backend.embedding_config import call_embedding_api, get_active_embedding_config
try:
    from evidence_locator import locate_evidence
except ImportError:
    try:
        from backend.evidence_locator import locate_evidence
    except ImportError:
        locate_evidence = None

def _get_embedder():
    config = get_active_embedding_config()
    embeddings = call_embedding_api(["Spark embedding warmup"], is_query=True)
    if not embeddings or not embeddings[0]:
        raise RuntimeError(
            f"Embedding warmup failed: no vector returned for {config.get('embedding_model_id')}"
        )
    dimension = len(embeddings[0])
    print(
        f"[Spark Query] Embedding API ready: "
        f"{config.get('embedding_model_id')} "
        f"provider={config.get('embedding_provider')} "
        f"base={config.get('embedding_base_url') or config.get('embedding_model_path')} "
        f"dimension={dimension}"
    )
    return {
        "config": config,
        "dimension": dimension,
    }
@lru_cache(maxsize=QUERY_EMBED_CACHE_SIZE)
def _encode_query_cached(question: str) -> tuple[float, ...]:
    text = question.strip()
    if not text:
        return tuple()
    embeddings = call_embedding_api([text], is_query=True)
    if not embeddings or not embeddings[0]:
        config = get_active_embedding_config()
        raise RuntimeError(
            f"Query embedding failed: no vector returned from {config.get('embedding_model_id')}"
        )
    return tuple(float(v) for v in embeddings[0])
def _get_collection():
    global _chroma_client, _chroma_collection
    if _chroma_collection is None:
        _chroma_client = chromadb.PersistentClient(path=str(CHROMA_FOLDER))
        _chroma_collection = _chroma_client.get_or_create_collection(
            name=CHROMA_COLLECTION,
            metadata={"hnsw:space": "cosine"}
        )
    return _chroma_collection
def retrieve(question: str, department: str = None) -> list[dict]:
    try:
        collection = _get_collection()
        active_paths = get_active_source_paths()
        if not active_paths:
            print("[Spark Retrieve] no active source paths")
            return []
    except Exception as exc:
        print(f"[Spark Retrieve] setup failed, using BM25 fallback: {exc}")
        print(traceback.format_exc())
        return []
    try:
        q_vec = list(_encode_query_cached(_norm_text(question)))
    except Exception as exc:
        print(f"[Spark Query] Vector retrieval unavailable, using BM25 fallback: {exc}")
        print(traceback.format_exc())
        return []
    where_clause = {"department": department} if department else None
    try:
        results = collection.query(
            query_embeddings=[q_vec],
            n_results=TOP_K,
            where=where_clause,
            include=["documents", "metadatas", "distances"]
        )
    except Exception as exc:
        print(f"[Spark Query] Chroma query failed, using BM25 fallback: {exc}")
        print(traceback.format_exc())
        return []
    if not results["ids"]:
        return []
    scored = []
    for chunk_id, doc, meta, distance in zip(
        results["ids"][0],
        results["documents"][0],
        results["metadatas"][0],
        results["distances"][0]
    ):
        source_path = meta.get("source_path", meta.get("source", "Unknown"))
        if source_path not in active_paths:
            continue
        score = round(1.0 - (distance / 2.0), 4)
        scored.append({
            "chunk_id": chunk_id,
            "document_id": meta.get("document_id"),
            "text": doc,
            "source": meta.get("source", "Unknown"),
            "source_name": meta.get("source_title", meta.get("source", "Unknown")),
            "source_title": meta.get("source_title", Path(meta.get("source", "Unknown")).stem.replace("_", " ")),
            "source_path": meta.get("source_path", meta.get("source", "Unknown")),
            "source_title": meta.get("source_title", Path(meta.get("source", "Unknown")).stem.replace("_", " ")),
            "source_fingerprint": meta.get("source_fingerprint"),
            "last_modified": meta.get("last_modified"),
            "file_size": meta.get("file_size"),
            "score": score,
            "department": meta.get("department", "General"),
            "application": meta.get("application", "Knowledge Assistant"),
            "chunk_index": meta.get("chunk_index", 0),
            "page_number": meta.get("page_number"),
            "section_title": meta.get("section_title"),
            "parent_section_title": meta.get("parent_section_title"),
            "chunk_type": meta.get("chunk_type", "body"),
            "vector_eligible": meta.get("vector_eligible", 1),
            "vector_skip_reason": meta.get("vector_skip_reason"),
            "embedding_model_id": meta.get("embedding_model_id"),
            "embedding_dimension": meta.get("embedding_dimension"),
            "quality_flags": meta.get("quality_flags"),
            "extraction_method": meta.get("extraction_method"),
            "has_text_layer": meta.get("has_text_layer"),
            "ocr_confidence": meta.get("ocr_confidence"),
            "char_count": meta.get("char_count"),
            "word_count": meta.get("word_count"),
            "file_type": meta.get("file_type"),
            "sheet_name": meta.get("sheet_name"),
            "range_ref": meta.get("range_ref"),
            "row_start": meta.get("row_start"),
            "row_end": meta.get("row_end"),
            "col_start": meta.get("col_start"),
            "col_end": meta.get("col_end"),
            "headers": meta.get("headers"),
            "has_structured_preview": meta.get("has_structured_preview"),
        })
    print(f"[Spark Retrieve] vector_chunks={len(scored)} department={department or 'all'}")
    return scored
def retrieve_bm25(keyword_query: str) -> list[dict]:
    results = search_chunks_fts(_expand_keyword_query(keyword_query), limit=TOP_K)
    if not results:
        merged: dict[str, dict] = {}
        for fallback_query in _bm25_fallback_queries(keyword_query):
            for item in search_chunks_fts(fallback_query, limit=TOP_K):
                key = _chunk_identity(item)
                if key not in merged or float(item.get("score", 0.0) or 0.0) > float(merged[key].get("score", 0.0) or 0.0):
                    merged[key] = item
        if merged:
            results = sorted(merged.values(), key=lambda row: float(row.get("score", 0.0) or 0.0), reverse=True)[:TOP_K]
            print(f"[Spark Query BM25] fallback_queries={_bm25_fallback_queries(keyword_query)} results={len(results)}")
    return results
def get_retrieval_preview(question: str, department: str = None, limit: int = 8) -> list[dict[str, Any]]:
    vector_chunks = retrieve(question, department=department)
    bm25_chunks = retrieve_bm25(question)
    if not vector_chunks:
        vector_chunks = bm25_chunks
    quality_vector = vector_chunks
    reranked = _rerank_retrieval_chunks(question, quality_vector, bm25_chunks)
    preview: list[dict[str, Any]] = []
    for chunk in reranked[:limit]:
        preview.append({
            "document": chunk.get("source_title") or chunk.get("source_name") or chunk.get("source"),
            "source_path": chunk.get("source_path"),
            "page_number": chunk.get("page_number"),
            "chunk_index": chunk.get("chunk_index"),
            "chunk_id": chunk.get("chunk_id"),
            "section_title": chunk.get("section_title"),
            "parent_section_title": chunk.get("parent_section_title"),
            "chunk_type": chunk.get("chunk_type"),
            "quality_flags": chunk.get("quality_flags"),
            "vector_score": round(float(chunk.get("vector_score", 0.0) or 0.0), 4),
            "bm25_score": round(float(chunk.get("bm25_score", 0.0) or 0.0), 4),
            "rerank_score": round(float(chunk.get("rerank_score", 0.0) or 0.0), 4),
            "score": round(float(chunk.get("score", 0.0) or 0.0), 4),
            "selection_reason": chunk.get("selection_reason", "baseline"),
            "text": chunk.get("text", "")[:300],
        })
    return preview
def get_vector_sqlite_drift() -> dict[str, Any]:
    collection = _get_collection()
    active_source_paths = get_active_source_paths()
    chroma = collection.get(include=["metadatas"])
    chroma_source_paths: set[str] = set()
    for meta in chroma.get("metadatas", []) or []:
        if not meta:
            continue
        source_path = meta.get("source_path") or meta.get("source")
        if source_path:
            chroma_source_paths.add(str(source_path))
    sqlite_sources_missing_vectors = sorted(active_source_paths - chroma_source_paths)
    orphan_chroma_sources = sorted(chroma_source_paths - active_source_paths)
    sqlite_stats = get_document_stats()
    return {
        "orphan_chroma_sources": orphan_chroma_sources,
        "sqlite_sources_missing_vectors": sqlite_sources_missing_vectors,
        "vector_chunk_count": collection.count(),
        "sqlite_chunk_count": sqlite_stats["total_chunks"],
        "sqlite_vector_eligible_chunks": sqlite_stats["total_vector_eligible_chunks"],
        "drift_detected": collection.count() != sqlite_stats["total_vector_eligible_chunks"]
    }
def _reciprocal_rank_fusion(
    vector_results: list[dict],
    bm25_results: list[dict],
    k: int = 60,
    vector_weight: float = 0.6,
    bm25_weight: float = 0.4,
) -> list[dict]:
    fused: dict[str, dict] = {}
    for rank, item in enumerate(vector_results):
        src = item["source"]
        rrf = vector_weight / (k + rank + 1)
        if src not in fused:
            fused[src] = {**item, "rrf_score": 0.0, "chunks": []}
        fused[src]["rrf_score"] += rrf
        fused[src]["chunks"].append(item)
    for rank, item in enumerate(bm25_results):
        src = item["source"]
        rrf = bm25_weight / (k + rank + 1)
        if src not in fused:
            fused[src] = {**item, "rrf_score": 0.0, "chunks": []}
        fused[src]["rrf_score"] += rrf
        fused[src]["chunks"].append(item)
    merged = sorted(fused.values(), key=lambda x: x["rrf_score"], reverse=True)
    return merged[:TOP_K]
def _chunk_identity(chunk: dict) -> str:
    chunk_id = chunk.get("chunk_id")
    if chunk_id:
        return str(chunk_id)
    source_path = chunk.get("source_path") or chunk.get("source") or "Unknown"
    chunk_index = int(chunk.get("chunk_index", 0))
    text_key = _norm_text(chunk.get("text", ""))[:180]
    return hashlib.sha256(f"{source_path}|{chunk_index}|{text_key}".encode("utf-8")).hexdigest()[:24]
def _rerank_retrieval_chunks(question: str, vector_results: list[dict], bm25_results: list[dict]) -> list[dict]:
    combined: dict[str, dict] = {}
    def _upsert(item: dict, vector_score: float = 0.0, bm25_score: float = 0.0) -> None:
        key = _chunk_identity(item)
        existing = combined.get(key)
        payload = copy.deepcopy(existing or {})
        payload.update({
            "chunk_id": item.get("chunk_id") or payload.get("chunk_id") or key,
            "document_id": item.get("document_id", payload.get("document_id")),
            "text": item.get("text", payload.get("text", "")),
            "source": item.get("source", payload.get("source", "Unknown")),
            "source_name": item.get("source_name", payload.get("source_name", item.get("source", payload.get("source", "Unknown")))),
            "source_path": item.get("source_path", payload.get("source_path", payload.get("source", "Unknown"))),
            "source_title": item.get("source_title", payload.get("source_title", Path(item.get("source", "Unknown")).stem.replace("_", " "))),
            "source_fingerprint": item.get("source_fingerprint", payload.get("source_fingerprint")),
            "last_modified": item.get("last_modified", payload.get("last_modified")),
            "file_size": item.get("file_size", payload.get("file_size")),
            "department": item.get("department", payload.get("department", "General")),
            "application": item.get("application", payload.get("application", "Knowledge Assistant")),
            "chunk_index": item.get("chunk_index", payload.get("chunk_index", 0)),
            "page_number": item.get("page_number", payload.get("page_number")),
            "section_title": item.get("section_title", payload.get("section_title")),
            "parent_section_title": item.get("parent_section_title", payload.get("parent_section_title")),
            "chunk_type": item.get("chunk_type", payload.get("chunk_type", "body")),
            "sheet_name": item.get("sheet_name", payload.get("sheet_name")),
            "range_ref": item.get("range_ref", payload.get("range_ref")),
            "row_start": item.get("row_start", payload.get("row_start")),
            "row_end": item.get("row_end", payload.get("row_end")),
            "col_start": item.get("col_start", payload.get("col_start")),
            "col_end": item.get("col_end", payload.get("col_end")),
            "headers": item.get("headers", payload.get("headers")),
            "quality_flags": item.get("quality_flags", payload.get("quality_flags")),
            "extraction_method": item.get("extraction_method", payload.get("extraction_method")),
            "has_text_layer": item.get("has_text_layer", payload.get("has_text_layer")),
            "ocr_confidence": item.get("ocr_confidence", payload.get("ocr_confidence")),
            "char_count": item.get("char_count", payload.get("char_count")),
            "word_count": item.get("word_count", payload.get("word_count")),
            "vector_score": max(float(payload.get("vector_score", 0.0) or 0.0), float(vector_score or 0.0)),
            "bm25_score": max(float(payload.get("bm25_score", 0.0) or 0.0), float(bm25_score or 0.0)),
        })
        combined[key] = payload
    for item in vector_results:
        _upsert(item, vector_score=float(item.get("score", 0.0) or 0.0))
    for item in bm25_results:
        _upsert(item, bm25_score=float(item.get("score", 0.0) or 0.0))
    question_tokens = set(_tokenize_for_search(question))
    question_numbers = _extract_numbers(question)
    intents = _detect_question_intents(question)
    is_doc_discovery = _is_document_discovery_question(question)
    ranked: list[dict] = []
    for item in combined.values():
        text = item.get("text", "")
        source_title = item.get("source_title", "")
        overlap = _token_overlap_score(question, text)
        title_overlap = _token_overlap_score(question, source_title)
        question_sentence = _question_sentence_match_score(question, text[:400])
        vector_score = _clamp01(float(item.get("vector_score", 0.0) or 0.0))
        raw_bm25_score = float(item.get("bm25_score", 0.0) or 0.0)
        bm25_score = _clamp01(raw_bm25_score)
        if is_doc_discovery:
            score = (
                (vector_score * 0.22)
                + (bm25_score * 0.22)
                + (overlap * 0.22)
                + (title_overlap * 0.22)
                + (min(question_sentence / 20.0, 1.0) * 0.10)
            )
        else:
            score = (
                (vector_score * 0.30)
                + (bm25_score * 0.20)
                + (overlap * 0.28)
                + (title_overlap * 0.08)
                + (min(question_sentence / 20.0, 1.0) * 0.10)
            )
        if question_numbers and question_numbers & _extract_numbers(text):
            score += 0.08
        if _norm_text(question) in _norm_text(text):
            score += 0.12
        if source_title and any(tok in _norm_text(source_title) for tok in question_tokens):
            score += 0.04
        if is_doc_discovery and source_title:
            title_tokens = set(_tokenize_for_search(source_title))
            if question_tokens & title_tokens:
                score += 0.08
        adjustment, ranking_notes = _chunk_ranking_adjustments(item, intents, question)
        score += adjustment
        item["rerank_score_raw"] = round(score, 4)
        item["rerank_score"] = round(_clamp01(score), 4)
        item["retrieval_score"] = round(max(vector_score, bm25_score), 4)
        item["bm25_score_normalized"] = round(bm25_score, 4)
        item["ranking_notes"] = ranking_notes
        item["selection_reason"] = ", ".join(ranking_notes[:4]) if ranking_notes else "baseline"
        ranked.append(item)
    ranked.sort(key=lambda item: (
        item.get("rerank_score_raw", item.get("rerank_score", 0.0)),
        item.get("rerank_score", 0.0),
        item.get("retrieval_score", 0.0),
        item.get("vector_score", 0.0),
    ), reverse=True)
    return ranked
def _dedupe_source_chunks(chunks: list[dict]) -> list[dict]:
    unique: list[dict] = []
    seen: set[tuple[str, int, str]] = set()
    for chunk in sorted(chunks, key=lambda c: c.get("score", 0), reverse=True):
        key = (
            chunk.get("source", "Unknown"),
            int(chunk.get("chunk_index", 0)),
            _norm_text(chunk.get("text", ""))[:160],
        )
        if key in seen:
            continue
        seen.add(key)
        unique.append(chunk)
    return unique
def _dedupe_retrieval_chunks(chunks: list[dict]) -> list[dict]:
    unique: list[dict] = []
    seen: set[tuple[str, int, str]] = set()
    for chunk in sorted(chunks, key=lambda c: c.get("rerank_score", c.get("score", 0)), reverse=True):
        key = (
            str(chunk.get("source_path") or chunk.get("source") or "Unknown"),
            int(chunk.get("chunk_index", 0)),
            _norm_text(chunk.get("text", ""))[:160],
        )
        if key in seen:
            continue
        seen.add(key)
        unique.append(chunk)
    return unique
def _source_uses_evidence_only(source: dict) -> bool:
    return bool(
        source.get("evidence_anchor")
        and source.get("evidence_context")
        and float(source.get("evidence_confidence", 0) or 0) >= 0.55
    )
def _source_effective_context(source: dict) -> str:
    return (
        source.get("evidence_context")
        or source.get("evidence_anchor")
        or source.get("evidence_text")
        or source.get("snippet")
        or source.get("chunk_text")
        or ""
    )
def _sentence_supported_by_evidence(sentence: str, evidence_text: str) -> bool:
    sentence_norm = _norm_text(sentence)
    evidence_norm = _norm_text(evidence_text)
    if not sentence_norm:
        return False
    if not evidence_norm:
        return False
    sentence_tokens = set(_tokenize_for_search(sentence_norm))
    evidence_tokens = set(_tokenize_for_search(evidence_norm))
    if not sentence_tokens or not evidence_tokens:
        return False
    overlap = len(sentence_tokens & evidence_tokens) / max(len(sentence_tokens), 1)
    return overlap >= 0.45 or sentence_norm in evidence_norm
def _filter_answer_to_evidence(answer: str, evidence_text: str) -> str:
    sentences = _split_sentences(answer)
    if not sentences:
        return answer.strip()
    kept = [sentence.strip() for sentence in sentences if _sentence_supported_by_evidence(sentence, evidence_text)]
    if not kept:
        return ""
    return " ".join(kept).strip()
def _build_context_payload(merged: list[dict]) -> tuple[str, dict[str, int], dict[str, dict]]:
    context_chunks: list[str] = []
    source_index: dict[str, int] = {}
    seen_sources: dict[str, dict] = {}
    chunks_per_source: dict[str, int] = {}
    for item in merged[:MAX_CONTEXT_CHUNKS]:
        src = item.get("source_title") or item.get("source_name") or item.get("source") or "Unknown"
        if src not in source_index:
            source_index[src] = len(source_index) + 1
        count = chunks_per_source.get(src, 0)
        if count >= MAX_CHUNKS_PER_SOURCE:
            continue
        chunk_text = item.get("text", "").strip()
        if not chunk_text:
            continue
        chunks_per_source[src] = count + 1
        if src not in seen_sources or item.get("rerank_score", 0) > seen_sources[src].get("score", 0):
            sentences = _split_sentences(chunk_text)
            evidence_context = item.get("evidence_context") or ""
            evidence_anchor = item.get("evidence_anchor") or ""
            evidence_text = item.get("evidence_text") or ""
            snippet = item.get("snippet") or chunk_text
            use_evidence_only = _source_uses_evidence_only(item)
            effective_context = _source_effective_context(item) if use_evidence_only else (evidence_context or evidence_anchor or evidence_text or snippet or chunk_text)
            seen_sources[src] = {
                "name": src,
                "source_name": item.get("source_name", src),
                "source_title": item.get("source_title", src),
                "source_path": item.get("source_path", src),
                "document_id": item.get("document_id"),
                "snippet": snippet,
                "evidence_text": evidence_text or effective_context,
                "evidence_sentence": sentences[0].strip() if sentences else chunk_text,
                "evidence_anchor": evidence_anchor,
                "evidence_context": evidence_context,
                "evidence_confidence": float(item.get("evidence_locator_confidence", item.get("evidence_score", 0.0)) or 0.0),
                "use_evidence_only": use_evidence_only,
                "effective_context": effective_context,
                "evidence_score": 0.0,
                "chunk_text": chunk_text,
                "chunk_index": item.get("chunk_index", 0),
                "chunk_id": item.get("chunk_id"),
                "source_fingerprint": item.get("source_fingerprint"),
                "last_modified": item.get("last_modified"),
                "file_size": item.get("file_size"),
                "page_number": item.get("page_number"),
                "section_title": item.get("section_title"),
                "parent_section_title": item.get("parent_section_title"),
                "chunk_type": item.get("chunk_type", "body"),
                "sheet_name": item.get("sheet_name"),
                "range_ref": item.get("range_ref"),
                "row_start": item.get("row_start"),
                "row_end": item.get("row_end"),
                "col_start": item.get("col_start"),
                "col_end": item.get("col_end"),
                "headers": item.get("headers"),
                "quality_flags": item.get("quality_flags"),
                "extraction_method": item.get("extraction_method"),
                "has_text_layer": item.get("has_text_layer"),
                "ocr_confidence": item.get("ocr_confidence"),
                "char_count": item.get("char_count"),
                "word_count": item.get("word_count"),
                "application": item.get("application", "Knowledge Assistant"),
                "score": item.get("rerank_score", item.get("score", 0)),
                "vector_score": round(float(item.get("vector_score", 0.0) or 0.0), 4),
                "bm25_score": round(float(item.get("bm25_score", 0.0) or 0.0), 4),
                "rerank_score": round(float(item.get("rerank_score", 0.0) or 0.0), 4),
                "ranking_notes": item.get("ranking_notes", []),
                "selection_reason": item.get("selection_reason", "baseline"),
                "citation_num": source_index[src],
            }
        context_chunks.append(f"[Source {source_index[src]}: {src} | chunk {item.get('chunk_index', 0)}] {effective_context}")
    return "\n\n".join(context_chunks), source_index, seen_sources
def _fallback_answer() -> str:
    return "I don't have enough information in the available documents to answer that."
def _build_document_discovery_answer(seen_sources: dict[str, dict]) -> str:
    if not seen_sources:
        return _fallback_answer()
    lines: list[str] = []
    ordered = sorted(
        seen_sources.values(),
        key=lambda row: float(row.get("rerank_score", row.get("score", 0.0)) or 0.0),
        reverse=True,
    )
    for info in ordered[:MAX_CONTEXT_SOURCES]:
        title = (
            info.get("source_title")
            or info.get("source_name")
            or info.get("name")
            or "Untitled source"
        )
        citation = int(info.get("citation_num") or len(lines) + 1)
        evidence = (
            info.get("evidence_context")
            or info.get("evidence_anchor")
            or info.get("evidence_text")
            or info.get("effective_context")
            or info.get("snippet")
            or info.get("chunk_text")
            or ""
        )
        sentence = _split_sentences(evidence)
        support = sentence[0] if sentence else _norm_text(evidence)[:220]
        support = re.sub(r"\s+", " ", support).strip()
        if len(support) > 260:
            support = support[:257].rstrip() + "..."
        if support:
            lines.append(f"{title} [{citation}]: {support}")
        else:
            lines.append(f"{title} [{citation}]")
    return "\n".join(lines) if lines else _fallback_answer()
def _build_grounded_evidence_answer(question: str, seen_sources: dict[str, dict]) -> str:
    if not seen_sources:
        return _fallback_answer()
    q_norm = _norm_text(question)
    travel_limit = _is_travel_limit_question(question)
    ordered = sorted(
        seen_sources.values(),
        key=lambda row: float(row.get("rerank_score", row.get("score", 0.0)) or 0.0),
        reverse=True,
    )
    lines: list[str] = []
    for info in ordered[:2]:
        citation = int(info.get("citation_num") or len(lines) + 1)
        text = info.get("effective_context") or info.get("chunk_text") or info.get("snippet") or ""
        sentences = [sentence.strip() for sentence in _split_sentences(text) if sentence.strip()]
        if not sentences:
            continue
        best_sentence = max(sentences, key=lambda sentence: _evidence_sentence_score(question, "", sentence))
        if best_sentence:
            lines.append(f"{best_sentence} [{citation}]")
    if not lines:
        return _fallback_answer()
    if travel_limit and not re.search(r"(\$|\bdollar\b|\blimit\b|\bmaximum\b|\bper night\b|\brate\b|\bcap\b)", _norm_text(" ".join(lines))):
        lines.append("I did not find a specific hotel dollar limit in the retrieved travel policy text.")
    elif ("how do i" in q_norm or "how can i" in q_norm or "what do i do" in q_norm) and len(lines) == 1:
        lines.append("I did not find a more detailed step-by-step workflow in the retrieved policy text.")
    return " ".join(lines).strip()
def _safe_load_json_list(path: Path) -> list:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except (json.JSONDecodeError, OSError) as e:
        print(f"[Spark Query] Failed reading history file {path.name}: {e}")
        return []
def _atomic_write_json(path: Path, data: list):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=path.stem + ".", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_name, path)
    finally:
        if os.path.exists(tmp_name):
            try:
                os.remove(tmp_name)
            except OSError:
                pass
def _prune_history(history: list) -> list:
    pruned = history[-MAX_HISTORY_ENTRIES:] if len(history) > MAX_HISTORY_ENTRIES else list(history)
    if HISTORY_RETENTION_DAYS <= 0:
        return pruned
    cutoff = datetime.now() - timedelta(days=HISTORY_RETENTION_DAYS)
    retained = []
    for entry in pruned:
        timestamp = entry.get("timestamp", "")
        try:
            ts = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            retained.append(entry)
            continue
        if ts >= cutoff:
            retained.append(entry)
    return retained
async def _call_liminal(
    prompt: str,
    model: str = None,
    timeout_seconds: int = 120,
    retries: int = None,
    temperature: float = 0.1,
) -> str:
    import aiohttp
    target_model = model or LLM_MODEL
    retry_count = LIMINAL_RETRIES if retries is None else retries
    payload = {
        "model": target_model,
        "prompt": prompt,
        "temperature": temperature,
        "stream": False,
    }
    headers = {"Content-Type": "application/json"}
    if "localhost" not in LIMINAL_URL and "127.0.0.1" not in LIMINAL_URL:
        headers["Authorization"] = f"Bearer {LIMINAL_TOKEN}"
    last_error = ""
    for attempt in range(retry_count):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{LIMINAL_URL}/api/generate",
                    json=payload,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=timeout_seconds),
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return data.get("response", "").strip()
                    last_error = f"HTTP {resp.status}"
        except asyncio.TimeoutError:
            last_error = "Timeout"
        except Exception as e:
            last_error = str(e)
        if attempt < retry_count - 1:
            wait = LIMINAL_RETRY_BASE * (2 ** attempt)
            print(f"[Spark LLM] Attempt {attempt + 1} failed ({last_error}). Retrying in {wait:.1f}s…")
            await asyncio.sleep(wait)
    return f"Error: {last_error}"
async def _call_liminal_fast(prompt: str, model: str = None, temperature: float = 0.0) -> str:
    return await _call_liminal(
        prompt,
        model=model,
        timeout_seconds=FAST_LIMINAL_TIMEOUT_SECONDS,
        retries=FAST_LIMINAL_RETRIES,
        temperature=temperature,
    )
def _strip_thinking_output(text: str) -> str:
    import re
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL)
    boxed_match = re.search(r"\\boxed\{(.+?)\}", text, flags=re.DOTALL)
    if boxed_match:
        return boxed_match.group(1).strip()
    paragraphs = [p.strip() for p in text.strip().split("\n\n") if p.strip()]
    if len(paragraphs) > 1:
        last = paragraphs[-1]
        if len(last) < 300:
            return last
    return text.strip()
async def _condense_query(question: str, history: list) -> str:
    if not history:
        return question
    hist_txt = "\n".join([f"Q: {h['question']}\nA: {h['answer']}" for h in history[-2:]])
    prompt = (
        f"Rephrase the follow-up question as a standalone search query using context "
        f"from the history. Output ONLY the query.\n\n"
        f"History:\n{hist_txt}\n\n"
        f"Follow-up: {question}\n\n"
        f"Standalone Query:"
    )
    condensed = await _call_liminal(prompt, model=LLM_MODEL)
    if not condensed or condensed.startswith("Error"):
        return question
    condensed = _strip_thinking_output(condensed)
    if len(condensed) > 300:
        print(f"[Spark Query] Condensed query too long ({len(condensed)} chars), using original.")
        return question
    return condensed
def _scrub_hallucinated_math(text: str) -> str:
    import re
    sentence_patterns = [
        r'\d+\s*[x*×]\s*\d+',
        r'\(\s*\d+\s*[x*×]\s*\d+\s*\)',
        r'\d+\s*years?\s*[x*×]\s*\d+\s*days?',
        r'would have accrued\s+\d+',
        r'would have accumulated\s+\d+',
        r'totaling\s+\d+',
        r'so,?\s+after\s+\d+\s*years?[^.]{0,80}\d{2,}\s*days',
    ]
    sentence_re = re.compile('|'.join(sentence_patterns), re.IGNORECASE)
    sentences = re.split(r'(?<=[.!?])\s+', text.strip())
    clean = []
    for sent in sentences:
        if sentence_re.search(sent):
            print(f"[Spark] Scrubbed hallucinated math: {sent.strip()[:100]}")
        else:
            clean.append(sent)
    result = ' '.join(clean).strip()
    result = re.sub(r"\[Reference:.*?\]", "", result, flags=re.IGNORECASE)
    result = re.sub(r"Reference:.*", "", result, flags=re.IGNORECASE)
    result = result.strip()
    if not result:
        return "I don't have enough information in the available documents to answer that."
    return result
def _chunk_fallback_snippet(chunk_text: str) -> str:
    import re
    sentences = re.split(r'(?<=[.!?])\s+', chunk_text.strip())
    return ' '.join(sentences[:3]).strip()
def _quote_in_chunk(quote: str, chunk: str) -> bool:
    import re
    q = re.sub(r'\s+', ' ', quote.strip().lower())
    c = re.sub(r'\s+', ' ', chunk.lower())
    words = q.split()
    if len(words) <= 6:
        return q in c
    for win in [len(words), 8, 6]:
        if win > len(words):
            continue
        for i in range(len(words) - win + 1):
            phrase = ' '.join(words[i:i + win])
            if phrase in c:
                return True
    return False
def _is_malformed_answer(answer: str) -> bool:
    text = _norm_text(answer)
    if not text:
        return True
    raw = re.sub(r"\[[^\]]+\]", " ", str(answer or ""))
    raw = re.sub(r"\s+", " ", raw).strip()
    if not raw:
        return True
    lowered = raw.lower()
    if re.fullmatch(r"\[?\s*(?:source\s+\d+\s*:\s*chunk\s+\d+|chunk\s+\d+|source\s+\d+|\d+)\s*\]?", lowered):
        return True
    stripped = re.sub(r"(?i)\bsource\s+\d+\s*:\s*chunk\s+\d+\b", " ", raw)
    stripped = re.sub(r"(?i)\bsource\s+\d+\b", " ", stripped)
    stripped = re.sub(r"(?i)\bchunk\s+\d+\b", " ", stripped)
    stripped = re.sub(r"(?i)\breference\b", " ", stripped)
    stripped = re.sub(r"[\[\]\(\)\{\}:;,_\-]+", " ", stripped)
    stripped = re.sub(r"\s+", " ", stripped).strip()
    if not stripped:
        return True
    alpha_tokens = re.findall(r"[A-Za-z]{2,}", stripped)
    if not alpha_tokens:
        return True
    if lowered.startswith("[source") and len(alpha_tokens) <= 1:
        return True
    return False
def _extract_year_ranges(text: str) -> list[tuple[int, int]]:
    ranges: list[tuple[int, int]] = []
    for start, end in re.findall(r"\b(\d+)\s*[-–]\s*(\d+)\s+years?\b", text or "", flags=re.IGNORECASE):
        try:
            ranges.append((int(start), int(end)))
        except ValueError:
            continue
    return ranges
def _sentence_has_policy_group_terms(sentence: str) -> bool:
    s_norm = _norm_text(sentence)
    return any(
        term in s_norm
        for term in (
            "years of service",
            "accrue",
            "accrues",
            "accrued",
            "days per year",
            "rate",
            "limit",
            "maximum",
            "eligible",
        )
    )
def _evidence_sentence_score(question: str, answer: str, sentence: str) -> float:
    q_norm = _norm_text(question)
    a_norm = _norm_text(answer)
    s_norm = _norm_text(sentence)
    score = _question_sentence_match_score(question, sentence)
    score += _token_overlap_score(sentence, answer) * 4.0
    q_nums = {int(n) for n in _extract_numbers(question)}
    a_nums = {int(n) for n in _extract_numbers(answer)}
    s_nums = {int(n) for n in _extract_numbers(sentence)}
    if q_nums and s_nums:
        score += 1.5 * len(q_nums & s_nums)
    if a_nums and s_nums:
        score += 3.5 * len(a_nums & s_nums)
    if re.search(r"\byears?\s+of\s+service\b", s_norm):
        score += 4.0
    if re.search(r"\baccru\w*\b", s_norm):
        score += 3.0
    if re.search(r"\bdays?\s+per\s+year\b", s_norm):
        score += 3.0
    if re.search(r"\b(rate|limit|maximum|eligible)\b", s_norm):
        score += 1.5
    if any(term in s_norm for term in (
        "must report",
        "should report",
        "contact",
        "submit",
        "request",
        "approval",
        "notify",
        "email",
        "e-mail",
        "phone",
        "voice mail",
        "voicemail",
        "whistleblower",
        "hotline",
        "anonymous",
    )):
        score += 0.35
    if any(term in q_norm for term in ("hotline", "ethics", "anonymous", "misconduct", "whistleblower", "whistle blower")):
        if "whistleblower" in s_norm or "whistle blower" in s_norm:
            score += 10.0
        if "anonymous" in s_norm:
            score += 6.0
        if any(term in s_norm for term in ("report security related incidents", "must report", "should report", "contact", "email", "e-mail", "phone", "voice mail", "voicemail")):
            score += 4.0
        if "technology group maintains an incident response playbook" in s_norm:
            score -= 4.0
        if "ethical conduct" in s_norm and not any(term in s_norm for term in ("report", "whistleblower", "anonymous", "contact")):
            score -= 3.0
    question_year_refs = q_nums or {int(n) for n in re.findall(r"\b(\d+)\s*years?\b", q_norm)}
    for start, end in _extract_year_ranges(sentence):
        for q_num in question_year_refs:
            if start <= q_num <= end:
                score += 8.0
                break
    if any(term in q_norm for term in ("after", "at", "when", "upon")):
        if question_year_refs and (_extract_year_ranges(sentence) or re.search(r"\b\d+\s+years?\b", s_norm)):
            score += 2.5
    if "5 years" in q_norm and re.search(r"\b3\s*[-–]\s*5\s+years?\b", s_norm):
        score += 8.0
    if "5 years" in q_norm and re.search(r"\b5\s+years?\b", s_norm):
        score += 6.0
    if "after 5 years" in q_norm and re.search(r"\b3\s*[-–]\s*5\s+years?\b", s_norm):
        score += 6.0
    if len(sentence.strip()) < 25:
        score -= 0.5
    if not re.search(r"[A-Za-z]", a_norm):
        score -= 1.5
    return round(max(score, 0.0), 4)
def _should_return_paragraph_group(sentences: list[str]) -> bool:
    if len(sentences) <= 1:
        return True
    if len(sentences) > 4:
        return False
    strong_hits = 0
    for sentence in sentences:
        if _sentence_has_policy_group_terms(sentence):
            strong_hits += 1
        elif re.search(r"\b\d+\b", sentence):
            strong_hits += 1
    return strong_hits >= min(2, len(sentences))
def _extract_evidence_span(question: str, answer: str, chunk_text: str) -> str:
    paragraphs = [p.strip() for p in re.split(r"\n{2,}", (chunk_text or "").strip()) if p.strip()]
    if not paragraphs:
        return _chunk_fallback_snippet(chunk_text)
    best_group = None
    best_score = -1.0
    for paragraph in paragraphs:
        sentences = _split_sentences(paragraph)
        if not sentences:
            continue
        sentence_scores = [_evidence_sentence_score(question, answer, sentence) for sentence in sentences]
        if not sentence_scores:
            continue
        best_idx = max(range(len(sentences)), key=lambda idx: sentence_scores[idx])
        score = sentence_scores[best_idx]
        evidence_sentence = sentences[best_idx].strip()
        best_sentence = evidence_sentence
        evidence_text = evidence_sentence
        if _should_return_paragraph_group(sentences):
            evidence_text = " ".join(sentences).strip()
            score += 4.0
        else:
            group_start = best_idx
            group_end = best_idx
            if best_idx > 0:
                prev_sentence = sentences[best_idx - 1].strip()
                if _sentence_has_policy_group_terms(prev_sentence) or _token_overlap_score(prev_sentence, best_sentence) > 0.25:
                    group_start = best_idx - 1
            if best_idx + 1 < len(sentences):
                next_sentence = sentences[best_idx + 1].strip()
                if _sentence_has_policy_group_terms(next_sentence) or _token_overlap_score(next_sentence, best_sentence) > 0.25:
                    group_end = best_idx + 1
            if group_start != best_idx or group_end != best_idx:
                evidence_text = " ".join(sentences[group_start:group_end + 1]).strip()
                score += 1.5
        if score > best_score:
            best_score = score
            best_group = {
                "evidence_text": evidence_text,
                "evidence_sentence": evidence_sentence,
                "evidence_score": round(score, 4),
            }
    if not best_group:
        fallback = _chunk_fallback_snippet(chunk_text)
        return fallback
    return best_group["evidence_text"] or _chunk_fallback_snippet(chunk_text)
def _build_sentence_context_window(sentences: list[str], best_idx: int, max_sentences: int = 3, max_chars: int = 420) -> str:
    if not sentences:
        return ""
    safe_idx = max(0, min(int(best_idx), len(sentences) - 1))
    start = safe_idx
    end = safe_idx
    selected = [sentences[safe_idx].strip()]
    while (end - start + 1) < max_sentences:
        prev_candidate = sentences[start - 1].strip() if start > 0 else ""
        next_candidate = sentences[end + 1].strip() if end + 1 < len(sentences) else ""
        prev_score = 0.0
        if prev_candidate:
            prev_score += 1.0 if _sentence_has_policy_group_terms(prev_candidate) else 0.0
            prev_score += _token_overlap_score(prev_candidate, sentences[safe_idx])
        next_score = 0.0
        if next_candidate:
            next_score += 1.0 if _sentence_has_policy_group_terms(next_candidate) else 0.0
            next_score += _token_overlap_score(next_candidate, sentences[safe_idx])
        if not prev_candidate and not next_candidate:
            break
        choose_prev = prev_score > next_score
        candidate = prev_candidate if choose_prev else next_candidate
        if not candidate:
            candidate = next_candidate if prev_candidate == "" else prev_candidate
            choose_prev = bool(prev_candidate and not next_candidate)
        trial = " ".join(([candidate] + selected) if choose_prev else (selected + [candidate])).strip()
        if len(trial) > max_chars and len(selected) >= 1:
            break
        if choose_prev:
            start -= 1
            selected.insert(0, candidate)
        else:
            end += 1
            selected.append(candidate)
    return " ".join(part.strip() for part in selected if part.strip()).strip()
def _extract_evidence_details(question: str, answer: str, chunk_text: str) -> dict:
    paragraphs = [p.strip() for p in re.split(r"\n{2,}", (chunk_text or "").strip()) if p.strip()]
    if not paragraphs:
        fallback = _chunk_fallback_snippet(chunk_text)
        return {
            "evidence_text": fallback,
            "evidence_sentence": fallback,
            "evidence_anchor": fallback,
            "evidence_context": fallback,
            "evidence_score": 0.0,
        }
    best_group = None
    best_score = -1.0
    for paragraph in paragraphs:
        sentences = _split_sentences(paragraph)
        if not sentences:
            continue
        sentence_scores = [_evidence_sentence_score(question, answer, sentence) for sentence in sentences]
        if not sentence_scores:
            continue
        best_idx = max(range(len(sentences)), key=lambda idx: sentence_scores[idx])
        score = sentence_scores[best_idx]
        evidence_sentence = sentences[best_idx].strip()
        best_sentence = evidence_sentence
        evidence_text = evidence_sentence
        evidence_context = _build_sentence_context_window(sentences, best_idx)
        if _should_return_paragraph_group(sentences):
            evidence_text = " ".join(sentences).strip()
            score += 4.0
        else:
            group_start = best_idx
            group_end = best_idx
            if best_idx > 0:
                prev_sentence = sentences[best_idx - 1].strip()
                if _sentence_has_policy_group_terms(prev_sentence) or _token_overlap_score(prev_sentence, best_sentence) > 0.25:
                    group_start = best_idx - 1
            if best_idx + 1 < len(sentences):
                next_sentence = sentences[best_idx + 1].strip()
                if _sentence_has_policy_group_terms(next_sentence) or _token_overlap_score(next_sentence, best_sentence) > 0.25:
                    group_end = best_idx + 1
            if group_start != best_idx or group_end != best_idx:
                evidence_text = " ".join(sentences[group_start:group_end + 1]).strip()
                score += 1.5
        if score > best_score:
            best_score = score
            best_group = {
                "evidence_text": evidence_text or evidence_sentence,
                "evidence_sentence": evidence_sentence or evidence_text,
                "evidence_anchor": evidence_sentence or evidence_text,
                "evidence_context": evidence_context or evidence_text or evidence_sentence,
                "evidence_score": round(score, 4),
            }
    if not best_group:
        fallback = _chunk_fallback_snippet(chunk_text)
        return {
            "evidence_text": fallback,
            "evidence_sentence": fallback,
            "evidence_anchor": fallback,
            "evidence_context": fallback,
            "evidence_score": 0.0,
        }
    return best_group
async def _extract_precise_snippet(question: str, answer: str, source_name: str, chunk_text: str) -> str:
    return _extract_evidence_span(question, answer, chunk_text)
def _locate_source_evidence(question: str, answer: str, info: dict) -> dict:
    try:
        payload = {
            "question": question,
            "answer": answer,
            "chunk_text": info.get("text", ""),
            "snippet": info.get("snippet", ""),
            "page_number": info.get("page"),
            "chunk_id": info.get("chunk_id", ""),
            "section_title": info.get("section_title", ""),
        }

        result = locate_evidence(payload)

        if not result:
            return {}

        return {
            "evidence_anchor": result.get("anchor_text", ""),
            "evidence_context": result.get("context_text", ""),
            "evidence_match_type": result.get("match_type", ""),
            "evidence_confidence": result.get("confidence", 0),
        }

    except Exception as e:
        print("[EvidenceLocator ERROR]", e)
        return {}

def _question_sentence_match_score(question: str, sentence: str) -> float:
    q_norm = _norm_text(question)
    s_norm = _norm_text(sentence)
    q_tokens = set(_tokenize_for_search(question))
    s_tokens = set(_tokenize_for_search(sentence))
    if not q_tokens or not s_tokens:
        return 0.0
    stopwords = {
        "what", "who", "when", "where", "why", "how", "is", "are", "was", "were",
        "the", "a", "an", "of", "of", "of", "of", "for", "to", "and", "or", "in",
        "on", "at", "by", "with", "from", "this", "that", "these", "those", "be",
        "do", "does", "did", "can", "could", "should", "would", "will", "may",
    }
    q_focus = {tok for tok in q_tokens if tok not in stopwords}
    if not q_focus:
        q_focus = q_tokens
    overlap = q_focus & s_tokens
    score = len(overlap) * 2.5
    score += (len(overlap) / max(len(q_focus), 1)) * 4.0
    q_nums = _extract_numbers(question)
    s_nums = _extract_numbers(sentence)
    if q_nums:
        score += 4.0 * len(q_nums & s_nums)
    if re.search(r"\$\d", sentence):
        score += 1.5
    if any(term in q_norm for term in ("limit", "maximum", "max", "cap", "rate", "hotel", "travel")):
        if re.search(r"(not exceeding|maximum|up to|limit|per night|\$\d)", s_norm):
            score += 3.5
    if "hotel" in q_norm and "hotel" in s_norm:
        score += 2.5
    if "business travel" in q_norm and ("travel" in s_norm or "business" in s_norm):
        score += 1.5
    if len(sentence.strip()) < 25:
        score -= 0.5
    return round(max(score, 0.0), 4)
def _rescue_answer_from_context(question: str, retrieved_chunks: list[dict], answer: str = "") -> dict | None:
    if not retrieved_chunks:
        return None
    ranked_chunks = sorted(retrieved_chunks, key=lambda chunk: chunk.get("score", 0), reverse=True)
    best: dict | None = None
    for chunk in ranked_chunks[:4]:
        chunk_text = chunk.get("text", "")
        for sentence in _split_sentences(chunk_text):
            score = _evidence_sentence_score(question, answer or sentence, sentence)
            # Compare each candidate sentence against the question and the
            # surrounding chunk so PTO-style year ranges land on the right line.
            if re.search(r"\b\d+\s*[-–]\s*\d+\s+years?\b", _norm_text(sentence)):
                score += 1.5
            if not best or score > best["score"]:
                best = {
                    "answer": sentence.strip(),
                    "evidence_text": _extract_evidence_span(question, sentence, chunk_text),
                    "evidence_sentence": sentence.strip(),
                    "evidence_score": score,
                    "score": score,
                    "chunk_id": chunk.get("chunk_id"),
                    "source": chunk.get("source"),
                    "chunk_index": chunk.get("chunk_index", 0),
                }
    if not best or best["score"] < RESCUE_MIN_SCORE:
        return None
    return best
async def build_answer(
    question: str,
    context: str,
    history: list,
    source_index: dict = None,
    revision_notes: str | None = None,
    draft_answer: str | None = None,
    document_discovery: bool = False,
) -> str:
    hist_txt = "\n".join([f"Q: {h['question']}\nA: {h['answer']}" for h in history[-3:]])
    source_legend = ""
    if source_index:
        lines = [f"[{num}] = {name}" for name, num in source_index.items()]
        source_legend = "Source index:\n" + "\n".join(lines) + "\n\n"
    prompt = (
        f"System: You are Spark, Renasant Bank's internal policy assistant.\n"
        f"Rules:\n"
        f"1. Answer ONLY from the context provided. Do not add outside knowledge.\n"
        f"Answer ONLY from the quoted evidence span provided for each source. "
        f"Do not add claims that are not explicitly stated in the evidence span. "
        f"If a detail is not directly stated, do not include it.\n"
        f"2. Be concise and factual. Quote the policy directly when possible.\n"
        f"3. NEVER perform arithmetic, calculate totals, or extrapolate values. "
        f"If a number is not explicitly stated in the context, do not compute or infer it.\n"
        f"4. NEVER speculate about what an employee 'would have' accumulated or earned. "
        f"Report only what the policy states.\n"
        f"5. If the context does not contain enough information, say exactly: "
        f"\"I don't have enough information in the available documents to answer that.\"\n"
        f"6. Each time you use information from a source, insert its citation number inline "
        f"as [1], [2], [3], etc. immediately after the relevant sentence or clause. "
        f"Use the Source index below to match source filenames to numbers. "
        f"Do NOT list sources at the end — only use inline [N] markers.\n"
        f"7. DO NOT include 'Reference:' or '[Reference: ...]' sections.\n\n"
        f"{('Discovery mode: The user is asking which policy, procedure, or document discusses a topic. Answer with the document title(s), a short reason from the provided evidence, and inline citation numbers only. Do not use phrases like Source 1 or Source 2 in the answer.\\n\\n') if document_discovery else ''}"
        f"{source_legend}"
        f"{('Revision notes:\\n' + revision_notes + '\\n\\n') if revision_notes else ''}"
        f"{('Draft answer to revise:\\n' + draft_answer + '\\n\\n') if draft_answer else ''}"
        f"History:\n{hist_txt}\n\n"
        f"Context:\n{context}\n\n"
        f"Question: {question}\n\n"
        f"Spark Answer:"
    )
    raw = await _call_liminal(prompt)
    cleaned = _strip_thinking_output(raw)
    return _scrub_hallucinated_math(cleaned)
def _is_fallback_answer(answer: str) -> bool:
    return "i don't have enough information" in _norm_text(answer)
async def load_history(user: str, limit: int = 20) -> list:
    async with _history_lock:
        CONVERSATIONS_FOLDER.mkdir(parents=True, exist_ok=True)
        p = CONVERSATIONS_FOLDER / f"{user}.json"
        if not p.exists():
            return []
        data = _prune_history(_safe_load_json_list(p))
        return data if limit == 0 else data[-limit:]
async def _append_and_cap(user: str, entry: dict):
    async with _history_lock:
        CONVERSATIONS_FOLDER.mkdir(parents=True, exist_ok=True)
        p = CONVERSATIONS_FOLDER / f"{user}.json"
        history = []
        if p.exists():
            history = _safe_load_json_list(p)
        history.append(entry)
        history = _prune_history(history)
        _atomic_write_json(p, history)
        print(f"[Spark] History updated for {user}: {len(history)} entries")
async def append_conversation_with_trace(user: str, entry: dict):
    await _append_and_cap(user, entry)
async def update_feedback(user: str, timestamp: str, value: str):
    async with _history_lock:
        CONVERSATIONS_FOLDER.mkdir(parents=True, exist_ok=True)
        p = CONVERSATIONS_FOLDER / f"{user}.json"
        if not p.exists():
            return
        history = _safe_load_json_list(p)
        for e in history:
            if e["timestamp"] == timestamp:
                e["feedback"] = value
                break
        _atomic_write_json(p, history)
        try:
            db_update_query_feedback(user, timestamp, value)
        except Exception as exc:
            print(f"[Spark Query] Failed to update SQLite feedback: {exc}")
        print(f"[Spark] Feedback updated for {user} at {timestamp}")
def _normalize_admin_note(note: dict) -> dict:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    created_at = note.get("created_at") or note.get("timestamp") or now
    updated_at = note.get("updated_at") or created_at
    title = (note.get("title") or "").strip()
    body = (note.get("body") or note.get("note") or "").strip()
    area = (note.get("area") or "Planning").strip() or "Planning"
    status = (note.get("status") or "todo").strip().lower()
    if status not in {"todo", "doing", "blocked", "done"}:
        status = "todo"
    priority = str(note.get("priority") or "2").strip()
    if priority not in {"1", "2", "3"}:
        priority = "2"
    return {
        "id": note.get("id") or f"note_{uuid4().hex[:12]}",
        "title": title or "Untitled note",
        "body": body,
        "area": area,
        "status": status,
        "priority": priority,
        "linked_question": (note.get("linked_question") or "").strip(),
        "linked_source": (note.get("linked_source") or "").strip(),
        "created_at": created_at,
        "updated_at": updated_at,
        "author": (note.get("author") or "admin").strip() or "admin",
    }
async def load_admin_notes() -> list[dict]:
    async with _notes_lock:
        ADMIN_NOTES_FILE.parent.mkdir(parents=True, exist_ok=True)
        if not ADMIN_NOTES_FILE.exists():
            return []
        notes = _safe_load_json_list(ADMIN_NOTES_FILE)
        normalized = [_normalize_admin_note(note) for note in notes if isinstance(note, dict)]
        normalized.sort(key=lambda n: (n.get("updated_at") or n.get("created_at") or ""), reverse=True)
        return normalized
async def create_admin_note(payload: dict) -> dict:
    async with _notes_lock:
        ADMIN_NOTES_FILE.parent.mkdir(parents=True, exist_ok=True)
        notes = _safe_load_json_list(ADMIN_NOTES_FILE) if ADMIN_NOTES_FILE.exists() else []
        note = _normalize_admin_note(payload)
        notes.append(note)
        _atomic_write_json(ADMIN_NOTES_FILE, notes)
        return note
async def update_admin_note(note_id: str, payload: dict) -> dict | None:
    async with _notes_lock:
        ADMIN_NOTES_FILE.parent.mkdir(parents=True, exist_ok=True)
        if not ADMIN_NOTES_FILE.exists():
            return None
        notes = _safe_load_json_list(ADMIN_NOTES_FILE)
        updated = None
        for idx, note in enumerate(notes):
            if not isinstance(note, dict):
                continue
            if str(note.get("id")) != str(note_id):
                continue
            merged = dict(note)
            merged.update({k: v for k, v in payload.items() if v is not None})
            merged["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            normalized = _normalize_admin_note(merged)
            notes[idx] = normalized
            updated = normalized
            break
        if updated is not None:
            _atomic_write_json(ADMIN_NOTES_FILE, notes)
        return updated
async def get_all_analytics() -> dict:
    all_entries = get_recent_query_logs(limit=5000)
    if not all_entries:
        async with _history_lock:
            CONVERSATIONS_FOLDER.mkdir(parents=True, exist_ok=True)
            files = list(CONVERSATIONS_FOLDER.glob("*.json"))
            fallback_entries = []
        for f in files:
            data = _safe_load_json_list(f)
            scope = f.stem
            for entry in data:
                entry_copy = dict(entry)
                entry_copy["user_scope"] = scope
                fallback_entries.append(entry_copy)
        all_entries = fallback_entries
    def _source_label(item: Any) -> str:
        if isinstance(item, dict):
            return (
                item.get("source_title")
                or item.get("source_name")
                or item.get("name")
                or item.get("source")
                or item.get("source_path")
                or "Unknown"
            )
        return str(item)
    all_entries.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
    now = datetime.now()
    dates = [(now - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(13, -1, -1)]
    vol_map = {d: 0 for d in dates}
    for e in all_entries:
        ts = str(e.get("timestamp", ""))
        dt = ts.split()[0] if ts else ""
        if dt in vol_map:
            vol_map[dt] += 1
    heatmap: dict[str, list] = {}
    date_index = {d: i for i, d in enumerate(dates)}
    for e in all_entries:
        ts = str(e.get("timestamp", ""))
        dt = ts.split()[0] if ts else ""
        if dt not in date_index:
            continue
        d_idx = date_index[dt]
        for src in e.get("sources", []):
            label = _source_label(src)
            if label not in heatmap:
                heatmap[label] = [0] * 14
            heatmap[label][d_idx] += 1
    feedback_dist = {"up": 0, "down": 0, "none": 0}
    thumbs_up_questions = []
    thumbs_down_questions = []
    unanswered = []
    unanswered_seen = set()
    source_usage = Counter()
    for e in all_entries:
        answer = str(e.get("answer", ""))
        question = str(e.get("question", ""))
        sources = e.get("sources", []) or []
        is_fallback = _is_fallback_answer(answer)
        for src in sources:
            source_usage[_source_label(src)] += 1
        if is_fallback:
            q_key = _norm_text(question)
            if q_key not in unanswered_seen:
                unanswered_seen.add(q_key)
                unanswered.append({
                    "question": question,
                    "sources": sources,
                    "timestamp": e.get("timestamp", ""),
                })
        fb = e.get("feedback")
        if fb == "up":
            feedback_dist["up"] += 1
            thumbs_up_questions.append({
                "question": question,
                "timestamp": e.get("timestamp", ""),
                "sources": sources,
            })
        elif fb == "down":
            feedback_dist["down"] += 1
            thumbs_down_questions.append({
                "question": question,
                "timestamp": e.get("timestamp", ""),
                "sources": sources,
                "answer": answer,
            })
        else:
            feedback_dist["none"] += 1
    rated = feedback_dist["up"] + feedback_dist["down"]
    satisfaction_rate = round(feedback_dist["up"] / rated * 100, 1) if rated > 0 else None
    active_users = len({str(entry.get("user_scope", "local_user")) for entry in all_entries})
    doc_effectiveness = []
    if source_usage:
        max_uses = max(source_usage.values()) or 1
        for source, uses in sorted(source_usage.items(), key=lambda item: (item[1], item[0]), reverse=True):
            doc_effectiveness.append({
                "source": source,
                "uses": uses,
                "usage_share": round(uses / max_uses, 4),
            })
    try:
        admin_notes = await load_admin_notes()
    except Exception as exc:
        print(f"[Spark Query] Failed to load admin notes: {exc}")
        admin_notes = []
    note_status = Counter(note.get("status", "todo") for note in admin_notes)
    note_area = Counter(note.get("area", "Planning") for note in admin_notes)
    today_prefix = datetime.now().strftime("%Y-%m-%d")
    notes_today = sum(1 for note in admin_notes if str(note.get("created_at", "")).startswith(today_prefix))
    return {
        "volume_14d": [vol_map[d] for d in dates],
        "heatmap_14d": heatmap,
        "unanswered_count": len(unanswered),
        "unanswered_questions": unanswered,
        "feedback_dist": feedback_dist,
        "satisfaction_rate": satisfaction_rate,
        "thumbs_up_recent": thumbs_up_questions[-10:],
        "thumbs_down_recent": thumbs_down_questions[-10:],
        "doc_effectiveness": doc_effectiveness,
        "ops_health": {
            "total_logs": len(all_entries),
            "answer_not_found": len(unanswered),
            "active_users": active_users,
        },
        "recent_logs": all_entries[:50],
        "evaluation": None,
        "admin_notes": {
            "items": admin_notes,
            "summary": {
                "total": len(admin_notes),
                "today": notes_today,
                "status_counts": dict(note_status),
                "area_counts": dict(note_area),
            },
        },
    }
def _needs_condensation(question: str, history: list) -> bool:
    if not history:
        return False
    words = question.lower().split()
    if len(words) >= 8:
        return False
    reference_words = {
        "it", "its", "that", "this", "they", "them", "their", "those",
        "same", "above", "previous", "more", "also", "too", "another",
        "what", "about", "how", "and", "but", "so",
    }
    return any(w in reference_words for w in words)
async def query_spark(question: str, user: str = "local_user") -> tuple[dict, dict]:
    wall_start = time.perf_counter()
    timing: dict[str, int] = {}
    fallback_used = False
    grounded_evidence_fallback_used = False
    question_intents = _detect_question_intents(question)
    is_reporting_intent = bool(question_intents.get("reporting_intent"))
    is_doc_discovery = _is_document_discovery_question(question)
    is_procedural = _is_procedural_question(question)
    if is_doc_discovery:
        print(f"[Spark Query] Document discovery mode triggered for question='{question}'")
    if is_procedural:
        print(f"[Spark Query] Procedural mode triggered for question='{question}'")
    t = time.perf_counter()
    history = await load_history(user, limit=20)
    timing["history_load_ms"] = _ms(t)
    loop = asyncio.get_event_loop()
    t = time.perf_counter()
    search_query = question
    if _needs_condensation(question, history):
        condense_coro = _condense_query(question, history)
        fts_task = loop.run_in_executor(None, retrieve_bm25, question)
        vector_query, fts_chunks = await asyncio.gather(condense_coro, fts_task)
        if vector_query != question:
            print(f"[Spark Query] Condensed for vector: '{vector_query}'")
        search_query = vector_query
        vector_chunks = await loop.run_in_executor(None, retrieve, search_query)
        timing["condense_ms"] = _ms(t)
        if not vector_chunks:
            vector_chunks = fts_chunks
            fallback_used = True
    else:
        fts_task = loop.run_in_executor(None, retrieve_bm25, question)
        vector_task = loop.run_in_executor(None, retrieve, question)
        fts_chunks, vector_chunks = await asyncio.gather(fts_task, vector_task)
        timing["condense_ms"] = 0
        if not vector_chunks:
            vector_chunks = fts_chunks
            fallback_used = True
    timing["retrieval_ms"] = _ms(t)
    t = time.perf_counter()
    quality_vector = vector_chunks
    quality_bm25 = fts_chunks[:TOP_K]
    merged = _reciprocal_rank_fusion(quality_vector, quality_bm25)
    reranked_chunks = _rerank_retrieval_chunks(search_query, quality_vector, quality_bm25)
    if not reranked_chunks:
        reranked_chunks = fts_chunks[:5]
        fallback_used = True
    retrieved_chunks = _dedupe_retrieval_chunks(reranked_chunks)
    for idx, item in enumerate(reranked_chunks[:5], start=1):
        print(
            "[Spark Query BM25] "
            f"rank={idx} "
            f"doc={item.get('source_title') or item.get('source_name') or item.get('source')} "
            f"raw={round(float(item.get('bm25_score', 0.0) or 0.0), 4)} "
            f"normalized={round(float(item.get('bm25_score_normalized', 0.0) or 0.0), 4)} "
            f"rerank={item.get('rerank_score')}"
        )
    context, source_index, seen_sources = _build_context_payload(reranked_chunks)
    print(
        "[Spark Query Counts] "
        f"doc_discovery={is_doc_discovery} "
        f"vector_chunks={len(vector_chunks)} "
        f"bm25_chunks={len(fts_chunks)} "
        f"quality_vector={len(quality_vector)} "
        f"quality_bm25={len(quality_bm25)} "
        f"merged={len(merged)} "
        f"reranked={len(reranked_chunks)} "
        f"context_empty={not bool(context.strip())} "
        f"fallback_used={fallback_used}"
    )
    timing["merge_ms"] = _ms(t)
    t = time.perf_counter()
    topic_guard: dict[str, Any] = {"applied": False, "passed": True, "matched_terms": [], "missing_terms": [], "coverage": 1.0}
    if context.strip():
        topic_terms = _question_topic_terms(question)
        topic_ok, topic_debug = _context_covers_topic(topic_terms, retrieved_chunks)
        topic_guard = {"applied": bool(topic_terms), "passed": bool(topic_ok), **topic_debug}
        if topic_terms and not topic_ok and not is_doc_discovery:
            if is_procedural:
                print(
                    "[Spark Query] Topic guard relaxed for procedural question "
                    f"coverage={topic_guard.get('coverage')} "
                    f"missing={topic_guard.get('missing_terms')}"
                )
                answer = await build_answer(question, context, history, source_index=source_index, document_discovery=is_doc_discovery)
            else:
                answer = _fallback_answer()
                fallback_used = True
        elif is_doc_discovery:
            if topic_terms and not topic_ok:
                print(
                    "[Spark Query] Topic guard bypassed for document discovery "
                    f"coverage={topic_guard.get('coverage')} "
                    f"missing={topic_guard.get('missing_terms')}"
                )
            answer = _build_document_discovery_answer(seen_sources)
        else:
            answer = await build_answer(question, context, history, source_index=source_index, document_discovery=is_doc_discovery)
    else:
        answer = _fallback_answer()
        fallback_used = True
    timing["llm_answer_ms"] = _ms(t)
    if context.strip() and topic_guard.get("passed", True) and (_is_malformed_answer(answer) or _is_fallback_answer(answer)):
        rescue_info = _rescue_answer_from_context(question, retrieved_chunks, answer=answer)
        if rescue_info and not _is_malformed_answer(rescue_info.get("answer", "")):
            answer = rescue_info["answer"]
        elif seen_sources and (is_procedural or _is_travel_limit_question(question) or topic_guard.get("passed", True)):
            answer = _build_grounded_evidence_answer(question, seen_sources)
            grounded_evidence_fallback_used = True
            print("[Spark Query] Grounded evidence fallback used for retrieved context")
        else:
            answer = _fallback_answer()
            fallback_used = True
    if is_reporting_intent and seen_sources and not _is_fallback_answer(answer):
        answer_norm = _norm_text(answer)
        channel_terms = ("contact", "email", "e-mail", "phone", "report", "hotline", "whistleblower", "anonymous", "notify")
        requires_reporting_channel = any(term in _norm_text(question) for term in ("hotline", "ethics", "anonymous", "misconduct"))
        has_reporting_channel = any(term in answer_norm for term in channel_terms)
        has_specific_reporting_channel = any(term in answer_norm for term in ("hotline", "whistleblower", "anonymous", "email", "e-mail", "phone", "voice mail", "voicemail"))
        if not has_reporting_channel or (requires_reporting_channel and not has_specific_reporting_channel):
            answer = _build_grounded_evidence_answer(question, seen_sources)
            grounded_evidence_fallback_used = True
            print("[Spark Query] Reporting answer guard forced evidence sentence")
    if _is_fallback_answer(answer) or _is_malformed_answer(answer):
        print("[Spark Query] Final answer is fallback/malformed -> clearing sources")
        fallback_used = True
        seen_sources = {}
        source_index = {}
    t = time.perf_counter()
    for src, info in seen_sources.items():
        chunk_text = info.get("chunk_text", info.get("snippet", ""))
        evidence_details = _locate_source_evidence(question, answer, info)
        evidence_anchor = evidence_details.get("evidence_anchor") or _chunk_fallback_snippet(chunk_text)
        evidence_context = evidence_details.get("evidence_context") or evidence_anchor
        evidence_text = evidence_context or evidence_anchor or _chunk_fallback_snippet(chunk_text)
        evidence_sentence = evidence_context or evidence_anchor or evidence_text
        evidence_score = float(evidence_details.get("evidence_confidence", 0.0) or 0.0)
        info["snippet"] = evidence_text
        info["evidence_text"] = evidence_text
        info["evidence_sentence"] = evidence_sentence
        info["evidence_anchor"] = evidence_anchor
        info["evidence_context"] = evidence_context
        info["evidence_score"] = round(evidence_score, 4)
        if evidence_details.get("evidence_match_type"):
            info["evidence_match_type"] = evidence_details.get("evidence_match_type")
        if evidence_details.get("evidence_confidence") is not None:
            info["evidence_locator_confidence"] = round(float(evidence_details.get("evidence_confidence", 0.0) or 0.0), 4)
    timing["snippet_extract_ms"] = _ms(t)
    combined_evidence_text = " ".join(
        " ".join(
            str(info.get(field) or "")
            for field in ("evidence_context", "evidence_anchor", "evidence_text")
        )
        for info in seen_sources.values()
    ).strip()
    hotline_clarification = ""
    if is_reporting_intent and "hotline" in _norm_text(question):
        combined_reporting_text = _norm_text(
            combined_evidence_text
            or " ".join(str(info.get("chunk_text") or info.get("effective_context") or "") for info in seen_sources.values())
        )
        if "hotline" not in combined_reporting_text:
            hotline_clarification = "I did not find a separate ethics hotline in the retrieved policy text."
            if "hotline" in _norm_text(answer) or "implies" in _norm_text(answer):
                answer = _build_grounded_evidence_answer(question, seen_sources)
                grounded_evidence_fallback_used = True
    if combined_evidence_text and not is_doc_discovery and not grounded_evidence_fallback_used:
        filtered_answer = _filter_answer_to_evidence(answer, combined_evidence_text)
        if filtered_answer:
            answer = filtered_answer
    if hotline_clarification and hotline_clarification not in answer:
        answer = f"{hotline_clarification} {answer}".strip()
    timing["total_ms"] = _ms(wall_start)
    selected_evidence_debug = []
    for info in sorted(seen_sources.values(), key=lambda row: float(row.get("rerank_score", row.get("score", 0.0)) or 0.0), reverse=True):
        selected_evidence_debug.append({
            "document": info.get("source_title") or info.get("source_name") or info.get("name"),
            "source_path": info.get("source_path"),
            "page_number": info.get("page_number"),
            "chunk_index": info.get("chunk_index"),
            "chunk_id": info.get("chunk_id"),
            "section_title": info.get("section_title"),
            "parent_section_title": info.get("parent_section_title"),
            "chunk_type": info.get("chunk_type"),
            "sheet_name": info.get("sheet_name"),
            "range_ref": info.get("range_ref"),
            "quality_flags": info.get("quality_flags"),
            "vector_score": info.get("vector_score"),
            "bm25_score": info.get("bm25_score"),
            "rerank_score": info.get("rerank_score"),
            "selection_reason": info.get("selection_reason"),
        })
        if len(selected_evidence_debug) >= 5:
            break
    for item in selected_evidence_debug:
        print(
            "[Spark Query] Selected evidence "
            f"doc={item.get('document')} "
            f"path={item.get('source_path')} "
            f"page={item.get('page_number')} "
            f"chunk={item.get('chunk_index')} "
            f"type={item.get('chunk_type')} "
            f"score={item.get('rerank_score')}"
        )
    top_v = max((float(item.get("score", 0.0) or 0.0) for item in quality_vector), default=0.0)
    top_b_raw = max((float(item.get("score", 0.0) or 0.0) for item in quality_bm25), default=0.0)
    top_b = _clamp01(top_b_raw)
    vector_confidence = min(100, max(0, int(((top_v - MIN_VECTOR_SCORE) / 0.50) * 100))) if top_v > MIN_VECTOR_SCORE else 0
    bm25_confidence = min(100, max(0, int(top_b * 100)))
    rerank_confidence = min(100, max(0, int((float(retrieved_chunks[0].get("rerank_score", 0.0) or 0.0) * 100)))) if retrieved_chunks else 0
    confidence = max(vector_confidence, bm25_confidence, rerank_confidence)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    entry = {
        "timestamp": timestamp,
        "question": question,
        "answer": answer,
        "sources": list(seen_sources.keys()),
        "source_detail": list(seen_sources.values()),
        "trace": {
            "retrieval": {
                "live_query_file": __file__,
                "live_cwd": os.getcwd(),
                "vector_candidates": len(vector_chunks),
                "bm25_candidates": len(fts_chunks),
                "qualified_vector_candidates": len(quality_vector),
                "qualified_bm25_candidates": len(quality_bm25),
                "reranked_candidates": len(reranked_chunks),
                "context_chunks": len(context.split("\n\n")) if context.strip() else 0,
                "selected_evidence": selected_evidence_debug,
                "topic_guard": topic_guard,
                "fallback_used": fallback_used,
                "grounded_evidence_fallback_used": grounded_evidence_fallback_used,
                "document_discovery": is_doc_discovery,
                "procedural_question": is_procedural,
                "reporting_intent": is_reporting_intent,
                "access_request_intent": bool(question_intents.get("access_request_intent")),
                "access_removal_intent": bool(question_intents.get("access_removal_intent")),
            },
            "confidence": confidence,
            "confidence_detail": {
                "vector": vector_confidence,
                "bm25": bm25_confidence,
                "rerank": rerank_confidence,
                "top_vector_score": round(top_v, 4),
                "top_bm25_score_raw": round(top_b_raw, 4),
                "top_bm25_score_normalized": round(top_b, 4),
            },
            "timing_ms": timing,
            "latency_ms": timing["total_ms"],
        },
        "feedback": None,
    }
    return entry, seen_sources
async def finalize_query_trace(user: str, entry: dict, seen_sources: dict):
    entry_copy = copy.deepcopy(entry)
    await _append_and_cap(user, entry_copy)
    try:
        db_log_query(entry_copy, user_id=user)
    except Exception as exc:
        print(f"[Spark Query] Failed to log SQLite query entry: {exc}")
