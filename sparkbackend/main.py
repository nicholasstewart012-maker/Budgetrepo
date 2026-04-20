import sys
import subprocess
import os
import base64
import re
import time
import traceback
from datetime import datetime
from pathlib import Path
from threading import Lock
from dotenv import load_dotenv
import chromadb

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env", override=True)

from fastapi import FastAPI, HTTPException, Header, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel

sys.path.insert(0, str(Path(__file__).parent))
from query import query_spark, load_history, get_all_analytics, finalize_query_trace, get_vector_sqlite_drift, get_retrieval_preview
try:
    from spark_db import init_db, get_document_stats, get_document_overview, get_index_health, get_document_by_source_path, purge_document, purge_index_data
except ModuleNotFoundError:
    from backend.spark_db import init_db, get_document_stats, get_document_overview, get_index_health, get_document_by_source_path, purge_document, purge_index_data
from document_viewer import build_document_metadata, build_document_highlights, build_view_source_payload, serve_document_file, serve_document_inline

app = FastAPI(title="Spark", version="1.1.0")
print(f"[Spark Live] main.__file__={__file__} cwd={os.getcwd()}")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://renasant.sharepoint.com", "http://localhost:5173"],
    allow_methods=["*"],
    allow_headers=["*"],
)

FRONTEND     = Path(r"C:\Spark\frontend")
INTAKE_FOLDER = Path(os.getenv("INTAKE_FOLDER", r"C:\Spark\intake"))
CHROMA_FOLDER = Path(os.getenv("CHROMA_FOLDER", r"C:\Spark\chromadb"))
CHROMA_COLLECTION = os.getenv("CHROMA_COLLECTION", "spark_documents")
CONVERSATIONS_FOLDER = Path(os.getenv("CONVERSATIONS_FOLDER", r"C:\Spark\conversations"))
BACKEND_ASSISTANT_AVATAR_FOLDER = Path(__file__).parent / "assistant-avatars"
FRONTEND_ASSISTANT_AVATAR_FOLDER = Path(__file__).parent.parent / "spark-react" / "public" / "ui" / "assistant-avatars"

BACKEND_ASSISTANT_AVATAR_FOLDER.mkdir(parents=True, exist_ok=True)
FRONTEND_ASSISTANT_AVATAR_FOLDER.mkdir(parents=True, exist_ok=True)

app.mount("/ui/assistant-avatars", StaticFiles(directory=str(BACKEND_ASSISTANT_AVATAR_FOLDER)), name="assistant-avatars")

if FRONTEND.exists():
    app.mount("/ui", StaticFiles(directory=str(FRONTEND)), name="ui")


_ADMIN_INGEST_LOCK = Lock()
_ADMIN_INGEST_ACTIVE = False
_ADMIN_INGEST_START_TIME = 0.0

# ── /history performance cache ───────────────────────────────────────
# The conversations JSON file grows large (1MB+ for active users) and
# load_history() does a blocking json.load on every request. We cache
# the parsed list per-user, invalidating on file mtime change. Since
# history is only mutated by /query (which we can notify), cache hits
# are essentially free: reading .stat() is ~20us on Windows SSD.
_HISTORY_CACHE: dict[str, tuple[int, list]] = {}
_HISTORY_CACHE_LOCK = Lock()



@app.on_event("startup")
async def _warm_singletons():
    import asyncio
    from query import _get_embedder, _call_liminal, LLM_MODEL

    loop = asyncio.get_event_loop()

    init_db()

    print("[Spark Startup] Warming embedding API...")
    await loop.run_in_executor(None, _get_embedder)

    print(f"[Spark Startup] Warming LLM model ({LLM_MODEL})")
    await _call_liminal("Hi", model=LLM_MODEL)

    print("[Spark Startup] Warm-up complete. Ready for queries.")



async def append_conversation_with_trace(user: str, entry: dict):
    from query import _append_and_cap
    await _append_and_cap(user, entry)


def _require_admin(token: str | None):
    admin_token = os.getenv("ADMIN_TOKEN", "")
    if not admin_token:
        raise HTTPException(status_code=500, detail="ADMIN_TOKEN is not configured")
    if not token or token != admin_token:
        raise HTTPException(status_code=401, detail="Unauthorized")


def _resolve_intake_source_path(source_path: str) -> Path:
    if not source_path or not str(source_path).strip():
        raise HTTPException(status_code=400, detail="source_path is required")
    candidate = Path(str(source_path))
    if not candidate.is_absolute():
        candidate = INTAKE_FOLDER / candidate
    resolved = candidate.resolve()
    intake_root = INTAKE_FOLDER.resolve()
    if resolved != intake_root and intake_root not in resolved.parents:
        raise HTTPException(status_code=400, detail="source_path must stay within the intake folder")
    if not resolved.exists():
        raise HTTPException(status_code=404, detail="source file not found")
    return resolved


def _sanitize_avatar_filename(filename: str) -> str:
    raw_name = Path(str(filename or "assistant-avatar.png")).name
    stem = Path(raw_name).stem.strip() or "assistant-avatar"
    stem = re.sub(r"[^a-zA-Z0-9._-]+", "-", stem).strip("-_.") or "assistant-avatar"
    suffix = Path(raw_name).suffix.lower()
    if suffix not in {".png", ".jpg", ".jpeg"}:
        raise HTTPException(status_code=400, detail="Only PNG and JPG images are allowed")
    if suffix == ".jpeg":
        suffix = ".jpg"
    return f"{stem}{suffix}"


def _run_admin_reingest(source_path: str):
    global _ADMIN_INGEST_ACTIVE
    try:
        try:
            from ingest import run_ingestion
        except ModuleNotFoundError:  # pragma: no cover
            from backend.ingest import run_ingestion

        print(f"[Spark Admin] Reingest started for {source_path}")
        run_ingestion(target_source_path=source_path, force_reingest=True)
        print(f"[Spark Admin] Reingest finished for {source_path}")
    finally:
        with _ADMIN_INGEST_LOCK:
            _ADMIN_INGEST_ACTIVE = False
            global _ADMIN_INGEST_START_TIME
            _ADMIN_INGEST_START_TIME = 0.0


def _normalize_source_path(source_path: str) -> str:
    return str(source_path or "").replace("\\", "/").strip().lstrip("./")


def _delete_chroma_source(source_path: str) -> int:
    normalized = _normalize_source_path(source_path)
    if not normalized:
        return 0

    client = chromadb.PersistentClient(path=str(CHROMA_FOLDER))
    collection = client.get_or_create_collection(
        name=CHROMA_COLLECTION,
        metadata={"hnsw:space": "cosine"},
    )

    deleted = 0
    candidates = [normalized, normalized.lower(), normalized.replace("/", "\\")]
    seen: set[str] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        existing = collection.get(where={"source_path": candidate})
        ids = existing.get("ids") or []
        if ids:
            collection.delete(ids=ids)
            deleted += len(ids)
    return deleted


def _delete_all_chroma_vectors() -> int | None:
    client = chromadb.PersistentClient(path=str(CHROMA_FOLDER))
    collection = client.get_or_create_collection(
        name=CHROMA_COLLECTION,
        metadata={"hnsw:space": "cosine"},
    )
    before = collection.count()
    if before <= 0:
        return 0

    try:
        collection.delete(where={})
        return before
    except Exception:
        # Fallback for Chroma variants that do not accept empty where.
        deleted = 0
        offset = 0
        batch_size = 500
        while True:
            # When deleting, always pull from offset 0 because the collection shrinks
            rows = collection.get(limit=batch_size, offset=0, include=[])
            ids = rows.get("ids") or []
            if not ids:
                break
            collection.delete(ids=ids)
            deleted += len(ids)
            if len(ids) < batch_size:
                break
        return deleted


class QueryRequest(BaseModel):
    question: str
    user:     str = "local_user"

class QueryResponse(BaseModel):
    answer:        str
    sources:       list[str]
    source_detail: list[dict]
    trace:         dict | None = None
    timestamp:     str  | None = None

class OpenSourceRequest(BaseModel):
    path:    str
    snippet: str = ""

class FeedbackRequest(BaseModel):
    timestamp: str
    feedback:  str
    user:      str = "local_user"


class DocumentHighlightRequest(BaseModel):
    path: str
    evidenceText: str = ""
    snippet: str = ""
    chunkText: str = ""
    answer: str = ""
    question: str = ""
    chunkIndex: int = 0
    chunkId: str = ""
    pageNumber: int | None = None
    extractionMethod: str | None = None
    hasTextLayer: bool | None = None
    ocrConfidence: float | None = None


class AdminNoteRequest(BaseModel):
    title: str
    body: str
    area: str = "Planning"
    status: str = "todo"
    priority: str = "2"
    linked_question: str = ""
    linked_source: str = ""
    author: str = "admin"


class AdminNotePatchRequest(BaseModel):
    title: str | None = None
    body: str | None = None
    area: str | None = None
    status: str | None = None
    priority: str | None = None
    linked_question: str | None = None
    linked_source: str | None = None
    author: str | None = None


class AdminReingestRequest(BaseModel):
    source_path: str


class AdminKnowledgeRemoveRequest(BaseModel):
    source_path: str


class RetrievalPreviewRequest(BaseModel):
    question: str
    department: str | None = None
    limit: int = 8


class AssistantAvatarUploadRequest(BaseModel):
    filename: str
    content_base64: str
    content_type: str = "image/png"



@app.get("/")
async def root():
    index = FRONTEND / "index.html"
    if index.exists():
        return FileResponse(str(index))
    return {"message": "Spark API running. No frontend found."}


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/status")
async def status():
    from query import _get_collection   
    collection = _get_collection()
    sqlite_stats = get_document_stats()
    documents = get_document_overview()
    return {
        "status": "ready" if sqlite_stats["total_documents"] > 0 else "empty",
        "sqlite_documents": sqlite_stats["total_documents"],
        "sqlite_active_documents": sqlite_stats["active_documents"],
        "sqlite_inactive_documents": sqlite_stats["inactive_documents"],
        "sqlite_chunks": sqlite_stats["total_chunks"],
        "last_ingestion_run": sqlite_stats["last_ingestion_run"],
        "vector_chunks": collection.count(),
        "documents": documents,
    }


@app.get("/admin/index-health")
async def admin_index_health(x_spark_token: str = Header(None)):
    _require_admin(x_spark_token)
    from query import _get_collection

    collection = _get_collection()
    vector_count = collection.count()
    sqlite_health = get_index_health(vector_chunks=vector_count)
    drift = get_vector_sqlite_drift()
    warning = bool(
        drift["orphan_chroma_sources"]
        or drift["sqlite_sources_missing_vectors"]
        or drift["drift_detected"]
        or sqlite_health["drift_detected"]
    )
    status = "ok"
    if sqlite_health["drift_detected"]:
        status = "stale"
    elif warning:
        status = "warning"
    return {
        "status": status,
        "ingestion_active": _ADMIN_INGEST_ACTIVE,
        "active_embedding_provider": sqlite_health["active_embedding_provider"],
        "active_embedding_base_url": sqlite_health["active_embedding_base_url"],
        "active_embedding_model_id": sqlite_health["active_embedding_model_id"],
        "active_embedding_normalized": sqlite_health["active_embedding_normalized"],
        "active_embedding_instruction": sqlite_health["active_embedding_instruction"],
        "active_embedding_config_hash": sqlite_health["active_embedding_config_hash"],
        "sqlite_vector_eligible_chunks": sqlite_health["sqlite_vector_eligible_chunks"],
        "chroma_vector_count": vector_count,
        "missing_embedding_metadata_count": sqlite_health["missing_embedding_metadata_count"],
        "model_mismatch_count": sqlite_health["model_mismatch_count"],
        "config_hash_missing_count": sqlite_health["config_hash_missing_count"],
        "config_mismatch_count": sqlite_health["config_mismatch_count"],
        "normalization_mismatch_count": sqlite_health["normalization_mismatch_count"],
        "dimension_mismatch_count": sqlite_health["dimension_mismatch_count"],
        "dimension_distribution": sqlite_health["dimension_distribution"],
        "drift_detected": sqlite_health["drift_detected"] or drift["drift_detected"],
        "drift_reasons": sqlite_health["drift_reasons"],
        "recommendation": sqlite_health["recommendation"],
        "sqlite": {
            "sqlite_documents": sqlite_health["sqlite_documents"],
            "sqlite_active_documents": sqlite_health["sqlite_active_documents"],
            "sqlite_inactive_documents": sqlite_health["sqlite_inactive_documents"],
            "sqlite_chunks": sqlite_health["sqlite_chunks"],
            "sqlite_active_chunks": sqlite_health["sqlite_active_chunks"],
            "sqlite_vector_eligible_chunks": sqlite_health["sqlite_vector_eligible_chunks"],
            "sqlite_vector_skipped_chunks": sqlite_health["sqlite_vector_skipped_chunks"],
            "vector_skip_reason_counts": sqlite_health["vector_skip_reason_counts"],
            "embedding_model_ids": sqlite_health["embedding_model_ids"],
            "ocr_documents": sqlite_health["ocr_documents"],
            "ocr_failed_documents": sqlite_health["ocr_failed_documents"],
            "zero_text_documents": sqlite_health["zero_text_documents"],
            "ocrChunkCount": sqlite_health["ocrChunkCount"],
            "ocrDocumentCount": sqlite_health["ocrDocumentCount"],
            "ocrPageCount": sqlite_health["ocrPageCount"],
            "ocrFailedPageCount": sqlite_health["ocrFailedPageCount"],
            "extractionMethodCounts": sqlite_health["extractionMethodCounts"],
            "page_extraction_counts": sqlite_health["page_extraction_counts"],
            "active_source_paths": sqlite_health["active_source_paths"],
            "recent_failed_documents": sqlite_health["recent_failed_documents"],
            "documents_with_zero_chunks": sqlite_health["documents_with_zero_chunks"],
        },
        "vector": {
            "vector_chunk_count": vector_count,
            "embedding_model_ids": sqlite_health["embedding_model_ids"],
        },
        "drift": drift,
        "last_ingestion_run": sqlite_health["last_ingestion_run"],
    }


@app.post("/admin/reingest")
async def admin_reingest(req: AdminReingestRequest, background_tasks: BackgroundTasks, x_spark_token: str = Header(None)):
    _require_admin(x_spark_token)
    resolved = _resolve_intake_source_path(req.source_path)
    try:
        relative_source_path = resolved.relative_to(INTAKE_FOLDER).as_posix()
    except ValueError:
        relative_source_path = resolved.name

    global _ADMIN_INGEST_ACTIVE, _ADMIN_INGEST_START_TIME
    with _ADMIN_INGEST_LOCK:
        now = time.time()
        if _ADMIN_INGEST_ACTIVE:
            # Stale lock cleanup (30 minutes)
            if now - _ADMIN_INGEST_START_TIME > 1800:
                print("[Spark Admin] Clearing stale ingestion lock")
                _ADMIN_INGEST_ACTIVE = False
            else:
                from fastapi.responses import JSONResponse
                return JSONResponse(
                    status_code=409,
                    content={
                        "status": "busy",
                        "message": "A reingest is already running. Please wait for it to finish."
                    }
                )

        _ADMIN_INGEST_ACTIVE = True
        _ADMIN_INGEST_START_TIME = now

    background_tasks.add_task(_run_admin_reingest, relative_source_path)
    return {
        "status": "started",
        "source_path": relative_source_path,
        "message": f"Re-ingest started for {relative_source_path}",
    }


@app.post("/admin/purge-index")
async def admin_purge_index(x_spark_token: str = Header(None)):
    _require_admin(x_spark_token)
    global _ADMIN_INGEST_ACTIVE, _ADMIN_INGEST_START_TIME

    with _ADMIN_INGEST_LOCK:
        if _ADMIN_INGEST_ACTIVE:
            raise HTTPException(status_code=409, detail="A re-ingest is currently running. Try again when it finishes.")
        _ADMIN_INGEST_ACTIVE = True
        _ADMIN_INGEST_START_TIME = time.time()

    try:
        vectors_deleted = _delete_all_chroma_vectors()
        db_summary = purge_index_data(reset_documents=True)
        return {
            "ok": True,
            "chunks_deleted": db_summary.get("chunks_deleted", 0),
            "pages_deleted": db_summary.get("pages_deleted", 0),
            "fts_rows_deleted": db_summary.get("fts_rows_deleted"),
            "vectors_deleted": vectors_deleted,
            "documents_reset": db_summary.get("documents_reset", 0),
            "sqlite_counts_after": db_summary.get("sqlite_counts_after"),
            "message": "Vector and chunk data deleted. Source files, document records, query logs, and ingestion history were preserved.",
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to purge index: {exc}") from exc
    finally:
        with _ADMIN_INGEST_LOCK:
            _ADMIN_INGEST_ACTIVE = False
            _ADMIN_INGEST_START_TIME = 0.0


@app.post("/admin/reingest-all")
async def admin_reingest_all(x_spark_token: str = Header(None)):
    _require_admin(x_spark_token)
    global _ADMIN_INGEST_ACTIVE, _ADMIN_INGEST_START_TIME

    with _ADMIN_INGEST_LOCK:
        if _ADMIN_INGEST_ACTIVE:
            raise HTTPException(status_code=409, detail="A re-ingest is currently running. Try again when it finishes.")
        _ADMIN_INGEST_ACTIVE = True
        _ADMIN_INGEST_START_TIME = time.time()

    try:
        try:
            from ingest import run_ingestion
        except ModuleNotFoundError:  # pragma: no cover
            from backend.ingest import run_ingestion

        summary = run_ingestion(force_reingest=True) or {}
        failed_items = summary.get("failed") or []
        skipped_items = summary.get("skipped") or []
        unsupported_items = summary.get("unsupported") or []
        return {
            "ok": bool(summary.get("ok", True)),
            "documents_processed": int(summary.get("documents_processed") or 0),
            "documents_indexed": int(summary.get("documents_indexed") or 0),
            "chunks_created": int(summary.get("chunks_created") or 0),
            "failed": failed_items + unsupported_items,
            "skipped": skipped_items,
            "unsupported": unsupported_items,
            "message": summary.get("message") or "Re-ingest completed.",
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to re-ingest intake data: {exc}") from exc
    finally:
        with _ADMIN_INGEST_LOCK:
            _ADMIN_INGEST_ACTIVE = False
            _ADMIN_INGEST_START_TIME = 0.0


@app.post("/admin/knowledge/remove")
async def admin_knowledge_remove(req: AdminKnowledgeRemoveRequest, x_spark_token: str = Header(None)):
    _require_admin(x_spark_token)
    source_path = _normalize_source_path(req.source_path)
    if not source_path:
        raise HTTPException(status_code=400, detail="source_path is required")

    document = get_document_by_source_path(source_path)
    if document is None:
        raise HTTPException(status_code=404, detail="Document not found")

    try:
        deleted_vectors = _delete_chroma_source(document["source_path"])
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to remove vector data: {exc}") from exc

    removed = purge_document(document["source_path"])
    if not removed:
        raise HTTPException(status_code=404, detail="Document already removed")

    return {
        "status": "ok",
        "source_path": document["source_path"],
        "document_id": document["document_id"],
        "deleted_vectors": deleted_vectors,
        "message": f"Removed {document['source_path']} from knowledge index",
    }


@app.post("/admin/assistant-avatar")
async def admin_assistant_avatar(req: AssistantAvatarUploadRequest, x_spark_token: str = Header(None)):
    _require_admin(x_spark_token)

    try:
        decoded = base64.b64decode(req.content_base64, validate=True)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid avatar image payload")

    filename = _sanitize_avatar_filename(req.filename)
    filename = f"{int(time.time() * 1000)}-{filename}"
    for folder in (BACKEND_ASSISTANT_AVATAR_FOLDER, FRONTEND_ASSISTANT_AVATAR_FOLDER):
        target = folder / filename
        target.write_bytes(decoded)

    return {
        "status": "ok",
        "filename": filename,
        "path": f"/ui/assistant-avatars/{filename}",
    }


@app.post("/admin/retrieval-preview")
async def admin_retrieval_preview(req: RetrievalPreviewRequest, x_spark_token: str = Header(None)):
    _require_admin(x_spark_token)
    if not req.question.strip():
        raise HTTPException(status_code=400, detail="question is required")

    import asyncio

    loop = asyncio.get_running_loop()
    limit = max(1, min(12, int(req.limit or 8)))
    preview = await loop.run_in_executor(
        None,
        lambda: get_retrieval_preview(req.question, department=req.department, limit=limit),
    )
    return {
        "question": req.question,
        "count": len(preview),
        "items": preview,
    }


@app.post("/query", response_model=QueryResponse)
async def query(req: QueryRequest, background_tasks: BackgroundTasks):
    print(f"[Spark Route] /query hit file={__file__} cwd={os.getcwd()} user={req.user}")
    if not req.question.strip():
        raise HTTPException(status_code=400, detail="Question cannot be empty")
    try:
        entry, seen_sources = await query_spark(req.question, user=req.user)
        background_tasks.add_task(finalize_query_trace, req.user, entry, seen_sources)
        return QueryResponse(**entry)
    except Exception as e:
        print(f"[Spark /query] Query failed for user={req.user}: {e}")
        print(traceback.format_exc())
        return QueryResponse(
            answer="I don't have enough information in the available documents to answer that.",
            sources=[],
            source_detail=[],
            trace={"error": str(e), "live_main_file": __file__, "live_cwd": os.getcwd()},
            timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )


@app.get("/history")
async def history(
    user:  str = "local_user",
    limit: int = Query(default=50, ge=1, le=200),   # Fix #11
):
    """
    Returns the user's conversation history. Uses an mtime-based in-memory
    cache so repeat calls (React StrictMode, tab refocus, etc.) don't re-parse
    the potentially-megabyte-scale JSON file on every hit.

    Cache invalidates automatically when the file is modified by /query.
    """
    try:
        print(f"[Spark Route] /history hit file={__file__} cwd={os.getcwd()} user={user} limit={limit}")
        perf_start = time.perf_counter()
        safe_user = re.sub(r"[^A-Za-z0-9_.-]", "_", user or "local_user")
        history_path = CONVERSATIONS_FOLDER / f"{safe_user}.json"

        # Cache hit check: use file mtime as cache key. If file doesn't exist
        # yet, mtime = 0 and we serve an empty list.
        try:
            mtime_ns = history_path.stat().st_mtime_ns
        except FileNotFoundError:
            mtime_ns = 0

        cached = None
        with _HISTORY_CACHE_LOCK:
            entry = _HISTORY_CACHE.get(safe_user)
            if entry and entry[0] == mtime_ns:
                cached = entry[1]

        if cached is not None:
            entries = cached[-limit:]
            elapsed = int((time.perf_counter() - perf_start) * 1000)
            if elapsed > 50:
                print(f"[Spark /history PERF] cache-hit took {elapsed}ms user={safe_user}")
            return {"user": user, "count": len(entries), "history": entries, "cached": True, "live_file": __file__, "cwd": os.getcwd()}

        # Cache miss: load_history does the actual file read + parse.
        # We cache the full list (not the slice) so different limits don't
        # all miss the cache.
        full = await load_history(user, limit=0)  # limit=0 = return all
        if not isinstance(full, list):
            full = list(full or [])

        with _HISTORY_CACHE_LOCK:
            _HISTORY_CACHE[safe_user] = (mtime_ns, full)

        entries = full[-limit:]
        elapsed = int((time.perf_counter() - perf_start) * 1000)
        if elapsed > 50:
            print(f"[Spark /history PERF] cache-miss took {elapsed}ms user={safe_user} entries={len(full)}")
        return {"user": user, "count": len(entries), "history": entries, "cached": False, "live_file": __file__, "cwd": os.getcwd()}
    except Exception as e:
        print(f"[Spark /history] Failed to load history for user={user}: {e}")
        print(traceback.format_exc())
        return {"user": user, "count": 0, "history": [], "cached": False, "warning": str(e), "live_file": __file__, "cwd": os.getcwd()}


@app.get("/view-source")
async def view_source(path: str, snippet: str = ""):
    try:
        return build_view_source_payload(path, snippet=snippet)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/document/file")
async def document_file(path: str):
    try:
        return serve_document_file(path)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/document/inline")
async def document_inline(path: str):
    try:
        return serve_document_inline(path)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/document/meta")
async def document_meta(path: str):
    try:
        return build_document_metadata(path)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/document/highlight")
async def document_highlight(req: DocumentHighlightRequest):
    try:
        return build_document_highlights(
            req.path,
            evidence_text=req.evidenceText,
            snippet=req.snippet,
            chunk_text=req.chunkText,
            answer=req.answer,
            question=req.question,
            chunk_index=req.chunkIndex,
            chunk_id=req.chunkId,
            page_number=req.pageNumber,
            extraction_method=req.extractionMethod,
            has_text_layer=req.hasTextLayer,
            ocr_confidence=req.ocrConfidence,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/open-source")
async def open_source(req: OpenSourceRequest):
    return {"message": "Use the in-app viewer instead."}


@app.post("/feedback")
async def feedback(req: FeedbackRequest):
    try:
        from query import update_feedback
        await update_feedback(req.user, req.timestamp, req.feedback)
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



@app.post("/ingest")
async def ingest(x_spark_token: str = Header(None)):
    _require_admin(x_spark_token)

    try:
        ingest_script = Path(__file__).parent / "ingest.py"
        result = subprocess.run(
            [sys.executable, str(ingest_script)],
            capture_output=True, text=True, timeout=300,
            cwd=str(Path(__file__).parent),
        )
        if result.returncode != 0:
            raise HTTPException(status_code=500, detail=result.stderr)
        return {"message": "Ingestion complete.", "log": result.stdout}
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="Ingestion timed out")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/admin/analytics")
async def admin_analytics(x_spark_token: str = Header(None)):
    _require_admin(x_spark_token)
    return await get_all_analytics()


@app.get("/admin/notes")
async def admin_notes(x_spark_token: str = Header(None)):
    _require_admin(x_spark_token)
    from query import load_admin_notes
    items = await load_admin_notes()
    return {"items": items, "count": len(items)}


@app.post("/admin/notes")
async def admin_notes_create(req: AdminNoteRequest, x_spark_token: str = Header(None)):
    _require_admin(x_spark_token)
    from query import create_admin_note
    note = await create_admin_note(req.model_dump())
    return {"note": note}


@app.patch("/admin/notes/{note_id}")
async def admin_notes_update(note_id: str, req: AdminNotePatchRequest, x_spark_token: str = Header(None)):
    _require_admin(x_spark_token)
    from query import update_admin_note
    note = await update_admin_note(note_id, req.model_dump(exclude_none=True))
    if note is None:
        raise HTTPException(status_code=404, detail="Note not found")
    return {"note": note}
