from __future__ import annotations

import csv
import json
import os
import sqlite3
import re
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, List, Optional
from datetime import datetime

try:
    from embedding_config import call_embedding_api, get_active_embedding_config
except ImportError:
    from backend.embedding_config import call_embedding_api, get_active_embedding_config
from uuid import uuid4

DB_PATH = Path(os.getenv("SPARK_DB_PATH", r"C:\Spark\spark.db"))


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA temp_store = MEMORY")
    return conn


@contextmanager
def get_connection():
    conn = _connect()
    try:
        yield conn
    finally:
        conn.close()


def init_db() -> None:
    schema = [
        """
        CREATE TABLE IF NOT EXISTS documents (
          document_id TEXT PRIMARY KEY,
          source_path TEXT NOT NULL UNIQUE,
          source_url TEXT,
          title TEXT NOT NULL,
          file_name TEXT NOT NULL,
          file_type TEXT NOT NULL,
          file_size INTEGER,
          file_hash TEXT NOT NULL,
          source_fingerprint TEXT NOT NULL,
          department TEXT DEFAULT 'General',
          application TEXT DEFAULT 'Knowledge Assistant',
          status TEXT DEFAULT 'active',
          audience TEXT DEFAULT 'all',
          is_admin_only INTEGER DEFAULT 0,
          effective_date TEXT,
          expiration_date TEXT,
          last_modified TEXT,
          last_ingested_at TEXT,
          page_count INTEGER,
          ingestion_status TEXT DEFAULT 'pending',
          ingestion_error TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS chunks (
          chunk_id TEXT PRIMARY KEY,
          document_id TEXT NOT NULL,
          chunk_index INTEGER NOT NULL,
          page_number INTEGER,
          section_title TEXT,
          parent_section_title TEXT,
          chunk_type TEXT DEFAULT 'body',
          quality_flags TEXT,
          extraction_method TEXT,
          has_text_layer INTEGER DEFAULT 1,
          ocr_confidence REAL,
           char_count INTEGER,
           word_count INTEGER,
           workbook_name TEXT,
           sheet_name TEXT,
           range_ref TEXT,
           row_start INTEGER,
           row_end INTEGER,
           col_start INTEGER,
           col_end INTEGER,
          headers TEXT,
          slide_number INTEGER,
          slide_title TEXT,
          block_index INTEGER,
          block_type TEXT,
          shape_name TEXT,
          shape_id TEXT,
          has_speaker_notes INTEGER DEFAULT 0,
          has_structured_preview INTEGER DEFAULT 0,
          text TEXT NOT NULL,
           token_count INTEGER,
           source_path TEXT NOT NULL,
          vector_eligible INTEGER DEFAULT 1,
          vector_skip_reason TEXT,
          embedding_model_id TEXT,
          embedding_model_path TEXT,
          embedding_dimension INTEGER,
          embedding_normalized INTEGER DEFAULT 0,
          embedding_created_at TEXT,
          created_at TEXT NOT NULL,
          FOREIGN KEY(document_id) REFERENCES documents(document_id) ON DELETE CASCADE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS document_pages (
          document_id TEXT NOT NULL,
          page_number INTEGER NOT NULL,
          text TEXT NOT NULL DEFAULT '',
          extraction_method TEXT NOT NULL,
          has_text_layer INTEGER NOT NULL DEFAULT 1,
          ocr_confidence REAL,
          char_count INTEGER NOT NULL DEFAULT 0,
          word_count INTEGER NOT NULL DEFAULT 0,
          PRIMARY KEY (document_id, page_number),
          FOREIGN KEY(document_id) REFERENCES documents(document_id) ON DELETE CASCADE
        )
        """,
        """
        CREATE VIRTUAL TABLE IF NOT EXISTS chunk_fts USING fts5(
          chunk_id UNINDEXED,
          title,
          section_title,
          text,
          department,
          application
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS ingestion_runs (
          run_id TEXT PRIMARY KEY,
          started_at TEXT NOT NULL,
          finished_at TEXT,
          status TEXT NOT NULL,
          files_seen INTEGER DEFAULT 0,
          files_ingested INTEGER DEFAULT 0,
          files_skipped INTEGER DEFAULT 0,
          files_failed INTEGER DEFAULT 0,
          chunks_created INTEGER DEFAULT 0,
          vector_chunks_created INTEGER DEFAULT 0,
          vector_skipped_count INTEGER DEFAULT 0,
          error TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS query_logs (
          query_id TEXT PRIMARY KEY,
          user_id TEXT,
          question TEXT NOT NULL,
          answer TEXT NOT NULL,
          sources_json TEXT,
          latency_ms INTEGER,
          created_at TEXT NOT NULL,
          feedback TEXT
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_documents_source_path ON documents(source_path)",
        "CREATE INDEX IF NOT EXISTS idx_documents_status ON documents(status)",
        "CREATE INDEX IF NOT EXISTS idx_documents_department ON documents(department)",
        "CREATE INDEX IF NOT EXISTS idx_documents_application ON documents(application)",
        "CREATE INDEX IF NOT EXISTS idx_chunks_document_id ON chunks(document_id)",
        "CREATE INDEX IF NOT EXISTS idx_chunks_source_path ON chunks(source_path)",
        "CREATE INDEX IF NOT EXISTS idx_chunks_page_number ON chunks(page_number)",
        "CREATE INDEX IF NOT EXISTS idx_document_pages_document_id ON document_pages(document_id)",
        "CREATE INDEX IF NOT EXISTS idx_document_pages_page_number ON document_pages(page_number)",
        "CREATE INDEX IF NOT EXISTS idx_document_pages_method ON document_pages(extraction_method)",
        "CREATE INDEX IF NOT EXISTS idx_query_logs_created_at ON query_logs(created_at)",
        "CREATE INDEX IF NOT EXISTS idx_query_logs_user_id ON query_logs(user_id)",
    ]

    with get_connection() as conn:
        for statement in schema:
            conn.execute(statement)
        _ensure_columns(
            conn,
            "chunks",
            {
                "extraction_method": "TEXT",
                "chunk_type": "TEXT DEFAULT 'body'",
                "parent_section_title": "TEXT",
                "quality_flags": "TEXT",
                "has_text_layer": "INTEGER DEFAULT 1",
                "ocr_confidence": "REAL",
                "char_count": "INTEGER",
                "word_count": "INTEGER",
                "workbook_name": "TEXT",
                "sheet_name": "TEXT",
                "range_ref": "TEXT",
                "row_start": "INTEGER",
                "row_end": "INTEGER",
                "col_start": "INTEGER",
                "col_end": "INTEGER",
                "headers": "TEXT",
                "slide_number": "INTEGER",
                "slide_title": "TEXT",
                "block_index": "INTEGER",
                "block_type": "TEXT",
                "shape_name": "TEXT",
                "shape_id": "TEXT",
                "has_speaker_notes": "INTEGER DEFAULT 0",
                "has_structured_preview": "INTEGER DEFAULT 0",
                "vector_eligible": "INTEGER DEFAULT 1",
                "vector_skip_reason": "TEXT",
                "embedding_model_id": "TEXT",
                "embedding_model_path": "TEXT",
                "embedding_dimension": "INTEGER",
                "embedding_normalized": "INTEGER DEFAULT 0",
                "embedding_provider": "TEXT",
                "embedding_base_url": "TEXT",
                "embedding_instruction": "TEXT",
                "embedding_config_hash": "TEXT",
                "embedding_created_at": "TEXT",
            },
        )
        _ensure_columns(
            conn,
            "ingestion_runs",
            {
                "vector_chunks_created": "INTEGER DEFAULT 0",
                "vector_skipped_count": "INTEGER DEFAULT 0",
            },
        )
        conn.commit()


def _ensure_columns(conn: sqlite3.Connection, table_name: str, columns: dict[str, str]) -> None:
    existing = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    existing_names = {str(row["name"]) for row in existing}
    for column_name, column_type in columns.items():
        if column_name in existing_names:
            continue
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")


def _row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return dict(row)


def _document_defaults(document: dict) -> dict[str, Any]:
    title = (document.get("title") or document.get("file_name") or "Untitled").strip()
    file_name = (document.get("file_name") or title or "document").strip()
    source_path = (document.get("source_path") or file_name).strip()
    file_type = (document.get("file_type") or Path(file_name).suffix.lower().lstrip(".") or "unknown").strip()
    source_url = (document.get("source_url") or "").strip() or None
    department = (document.get("department") or "General").strip() or "General"
    application = (document.get("application") or "Knowledge Assistant").strip() or "Knowledge Assistant"
    status = (document.get("status") or "active").strip().lower() or "active"
    audience = (document.get("audience") or "all").strip() or "all"
    return {
        "document_id": (document.get("document_id") or uuid4().hex).strip(),
        "source_path": source_path,
        "source_url": source_url,
        "title": title,
        "file_name": file_name,
        "file_type": file_type,
        "file_size": int(document.get("file_size") or 0) or None,
        "file_hash": (document.get("file_hash") or "").strip(),
        "source_fingerprint": (document.get("source_fingerprint") or "").strip(),
        "department": department,
        "application": application,
        "status": status,
        "audience": audience,
        "is_admin_only": int(document.get("is_admin_only") or 0),
        "effective_date": (document.get("effective_date") or "").strip() or None,
        "expiration_date": (document.get("expiration_date") or "").strip() or None,
        "last_modified": (document.get("last_modified") or "").strip() or None,
        "last_ingested_at": (document.get("last_ingested_at") or "").strip() or None,
        "page_count": int(document.get("page_count") or 0) or None,
        "ingestion_status": (document.get("ingestion_status") or "pending").strip() or "pending",
        "ingestion_error": (document.get("ingestion_error") or "").strip() or None,
    }


def upsert_document(document: dict) -> dict[str, Any]:
    payload = _document_defaults(document)
    columns = [
        "document_id", "source_path", "source_url", "title", "file_name", "file_type",
        "file_size", "file_hash", "source_fingerprint", "department", "application",
        "status", "audience", "is_admin_only", "effective_date", "expiration_date",
        "last_modified", "last_ingested_at", "page_count", "ingestion_status", "ingestion_error",
    ]
    values = [payload[column] for column in columns]
    placeholders = ", ".join("?" for _ in columns)
    updates = ", ".join(f"{column}=excluded.{column}" for column in columns[1:])

    with get_connection() as conn:
        conn.execute(
            f"""
            INSERT INTO documents ({", ".join(columns)})
            VALUES ({placeholders})
            ON CONFLICT(document_id) DO UPDATE SET {updates}
            """,
            values,
        )
        conn.commit()

    return payload


def get_document_by_source_path(source_path: str) -> dict[str, Any] | None:
    if not source_path:
        return None
    with get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM documents WHERE source_path = ? LIMIT 1",
            (source_path,),
        ).fetchone()
    return _row_to_dict(row)


def get_active_source_paths() -> set[str]:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT source_path FROM documents WHERE status = 'active'"
        ).fetchall()
    return {str(row["source_path"]) for row in rows if row["source_path"]}


def get_document_overview() -> list[dict[str, Any]]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT
              d.*,
              COUNT(c.chunk_id) AS chunk_count
            FROM documents d
            LEFT JOIN chunks c ON c.document_id = d.document_id
            GROUP BY d.document_id
            ORDER BY CASE WHEN d.status = 'active' THEN 0 ELSE 1 END, d.department, d.title
            """
        ).fetchall()
    documents = []
    for row in rows:
        doc = _row_to_dict(row)
        doc["chunks"] = int(doc.pop("chunk_count", 0) or 0)
        documents.append(doc)
    return documents


def get_document_chunk_count(document_id: str) -> int:
    if not document_id:
        return 0
    with get_connection() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS chunk_count FROM chunks WHERE document_id = ?",
            (document_id,),
        ).fetchone()
    return int(row["chunk_count"] or 0) if row else 0


def delete_document_chunks(document_id: str) -> int:
    if not document_id:
        return 0
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT chunk_id FROM chunks WHERE document_id = ?",
            (document_id,),
        ).fetchall()
        chunk_ids = [str(row["chunk_id"]) for row in rows]
        if chunk_ids:
            conn.executemany(
                "DELETE FROM chunk_fts WHERE chunk_id = ?",
                [(chunk_id,) for chunk_id in chunk_ids],
            )
        conn.execute("DELETE FROM chunks WHERE document_id = ?", (document_id,))
        conn.commit()
        return len(chunk_ids)


def delete_fts_for_document(document_id: str) -> int:
    if not document_id:
        return 0
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT chunk_id FROM chunks WHERE document_id = ?",
            (document_id,),
        ).fetchall()
        chunk_ids = [str(row["chunk_id"]) for row in rows]
        if chunk_ids:
            conn.executemany(
                "DELETE FROM chunk_fts WHERE chunk_id = ?",
                [(chunk_id,) for chunk_id in chunk_ids],
            )
            conn.commit()
        return len(chunk_ids)


def insert_chunk_fts(chunk: dict, document: dict) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO chunk_fts (chunk_id, title, section_title, text, department, application)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                chunk.get("chunk_id"),
                document.get("title") or document.get("file_name") or "Untitled",
                chunk.get("section_title"),
                chunk.get("text") or "",
                document.get("department") or "General",
                document.get("application") or "Knowledge Assistant",
            ),
        )
        conn.commit()


def insert_chunks(document_id: str, chunks: list[dict], document: dict) -> int:
    if not document_id or not chunks:
        return 0

    now = _now()
    rows = []
    fts_rows = []
    title = document.get("title") or document.get("file_name") or "Untitled"
    department = document.get("department") or "General"
    application = document.get("application") or "Knowledge Assistant"

    for chunk in chunks:
        rows.append(
            (
                chunk.get("chunk_id"),
                document_id,
                int(chunk.get("chunk_index") or 0),
                chunk.get("page_number"),
                chunk.get("section_title"),
                chunk.get("parent_section_title"),
                chunk.get("chunk_type") or "body",
                chunk.get("quality_flags"),
                chunk.get("extraction_method"),
                int(chunk.get("has_text_layer", 1) or 0),
                chunk.get("ocr_confidence"),
                int(chunk.get("char_count") or 0) or None,
                int(chunk.get("word_count") or 0) or None,
                chunk.get("workbook_name"),
                chunk.get("sheet_name"),
                chunk.get("range_ref"),
                int(chunk.get("row_start") or 0) or None,
                int(chunk.get("row_end") or 0) or None,
                int(chunk.get("col_start") or 0) or None,
                int(chunk.get("col_end") or 0) or None,
                json.dumps(chunk.get("headers") or [], ensure_ascii=False) if not isinstance(chunk.get("headers"), str) else chunk.get("headers"),
                int(chunk.get("slide_number") or 0) if chunk.get("slide_number") is not None else None,
                chunk.get("slide_title"),
                int(chunk.get("block_index") or 0) if chunk.get("block_index") is not None else None,
                chunk.get("block_type"),
                chunk.get("shape_name"),
                chunk.get("shape_id"),
                int(chunk.get("has_speaker_notes") or 0),
                int(chunk.get("has_structured_preview") or 0),
                chunk.get("text") or "",
                int(chunk.get("token_count") or 0) or None,
                chunk.get("source_path") or document.get("source_path"),
                int(chunk.get("vector_eligible", 1) or 0),
                chunk.get("vector_skip_reason"),
                chunk.get("embedding_model_id"),
                chunk.get("embedding_model_path"),
                chunk.get("embedding_dimension"),
                int(chunk.get("embedding_normalized", 0) or 0),
                chunk.get("embedding_provider"),
                chunk.get("embedding_base_url"),
                chunk.get("embedding_instruction"),
                chunk.get("embedding_config_hash"),
                chunk.get("embedding_created_at"),
                chunk.get("created_at") or now,
            )
        )
        fts_rows.append(
            (
                chunk.get("chunk_id"),
                title,
                chunk.get("section_title"),
                chunk.get("text") or "",
                department,
                application,
            )
        )

    with get_connection() as conn:
        conn.executemany(
            """
            INSERT OR REPLACE INTO chunks (
              chunk_id, document_id, chunk_index, page_number, section_title,
              parent_section_title, chunk_type, quality_flags, extraction_method,
              has_text_layer, ocr_confidence, char_count, word_count,
              workbook_name, sheet_name, range_ref, row_start, row_end, col_start, col_end,
              headers, slide_number, slide_title, block_index, block_type, shape_name, shape_id,
              has_speaker_notes, has_structured_preview, text, token_count, source_path,
              vector_eligible, vector_skip_reason, embedding_model_id, embedding_model_path,
              embedding_dimension, embedding_normalized, embedding_provider, embedding_base_url,
              embedding_instruction, embedding_config_hash, embedding_created_at, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.executemany(
            """
            INSERT OR REPLACE INTO chunk_fts (
              chunk_id, title, section_title, text, department, application
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            fts_rows,
        )
        conn.commit()
    return len(rows)


def delete_document_pages(document_id: str) -> int:
    if not document_id:
        return 0
    with get_connection() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS page_count FROM document_pages WHERE document_id = ?",
            (document_id,),
        ).fetchone()
        conn.execute("DELETE FROM document_pages WHERE document_id = ?", (document_id,))
        conn.commit()
    return int(row["page_count"] or 0) if row else 0


def insert_document_pages(document_id: str, pages: list[dict]) -> int:
    if not document_id or not pages:
        return 0

    rows = []
    for page in pages:
        rows.append(
            (
                document_id,
                int(page.get("page_number") or 0),
                page.get("text") or "",
                page.get("extraction_method") or "text_layer",
                int(page.get("has_text_layer", 1) or 0),
                page.get("ocr_confidence"),
                int(page.get("char_count") or 0),
                int(page.get("word_count") or 0),
            )
        )

    with get_connection() as conn:
        conn.executemany(
            """
            INSERT OR REPLACE INTO document_pages (
              document_id, page_number, text, extraction_method, has_text_layer,
              ocr_confidence, char_count, word_count
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
    return len(rows)


def purge_document(source_path: str) -> dict[str, Any] | None:
    document = get_document_by_source_path(source_path)
    if not document:
        return None

    with get_connection() as conn:
        conn.execute("DELETE FROM chunk_fts WHERE chunk_id IN (SELECT chunk_id FROM chunks WHERE document_id = ?)", (document["document_id"],))
        conn.execute("DELETE FROM chunks WHERE document_id = ?", (document["document_id"],))
        conn.execute("DELETE FROM documents WHERE document_id = ?", (document["document_id"],))
        conn.commit()
    return document


def purge_index_data(reset_documents: bool = True) -> dict:
    with get_connection() as conn:
        chunk_row = conn.execute("SELECT COUNT(*) AS c FROM chunks").fetchone()
        page_row = conn.execute("SELECT COUNT(*) AS c FROM document_pages").fetchone()
        fts_row = conn.execute("SELECT COUNT(*) AS c FROM chunk_fts").fetchone()

        chunks_deleted = int(chunk_row["c"] or 0) if chunk_row else 0
        pages_deleted = int(page_row["c"] or 0) if page_row else 0
        fts_deleted = int(fts_row["c"] or 0) if fts_row else 0

        conn.execute("DELETE FROM chunk_fts")
        conn.execute("DELETE FROM chunks")
        conn.execute("DELETE FROM document_pages")

        # FTS5 maintenance
        try:
            conn.execute("INSERT INTO chunk_fts(chunk_fts) VALUES('rebuild')")
            conn.execute("INSERT INTO chunk_fts(chunk_fts) VALUES('optimize')")
        except Exception as e:
            print(f"[Spark DB] FTS maintenance failed: {e}")

        documents_reset = 0
        if reset_documents:
            doc_row = conn.execute("SELECT COUNT(*) AS c FROM documents").fetchone()
            documents_reset = int(doc_row["c"] or 0) if doc_row else 0
            conn.execute(
                """
                UPDATE documents
                SET ingestion_status = 'purged',
                    ingestion_error = NULL,
                    page_count = NULL
                """
            )

        conn.commit()

        # Optional VACUUM (can be slow if DB is huge, but here it's usually small)
        vacuum_performed = False
        try:
            conn.execute("VACUUM")
            vacuum_performed = True
        except Exception as e:
            print(f"[Spark DB] VACUUM skipped: {e}")

    # Final counts for debug response
    after = get_sqlite_index_counts()

    return {
        "chunks_deleted": chunks_deleted,
        "pages_deleted": pages_deleted,
        "fts_rows_deleted": fts_deleted,
        "documents_reset": documents_reset,
        "vacuum_performed": vacuum_performed,
        "sqlite_counts_after": after
    }

def get_sqlite_index_counts() -> dict[str, int]:
    """Returns row counts for index-related tables including FTS shadow tables."""
    with get_connection() as conn:
        def get_count(table):
            try:
                row = conn.execute(f"SELECT COUNT(*) as c FROM {table}").fetchone()
                return int(row["c"] or 0) if row else 0
            except Exception:
                return -1

        return {
            "chunks": get_count("chunks"),
            "document_pages": get_count("document_pages"),
            "chunk_fts": get_count("chunk_fts"),
            "chunk_fts_content": get_count("chunk_fts_content"),
            "chunk_fts_docsize": get_count("chunk_fts_docsize"),
            "chunk_fts_idx": get_count("chunk_fts_idx"),
            "chunk_fts_data": get_count("chunk_fts_data"),
            "documents": get_count("documents"),
            "ingestion_runs": get_count("ingestion_runs"),
            "query_logs": get_count("query_logs"),
        }


def purge_missing_documents(active_source_paths: set[str]) -> list[str]:
    active = {str(path) for path in active_source_paths if path}
    with get_connection() as conn:
        rows = conn.execute("SELECT source_path FROM documents").fetchall()

    removed: list[str] = []
    for row in rows:
        source_path = str(row["source_path"])
        if source_path not in active:
            if purge_document(source_path):
                removed.append(source_path)
    return removed


def _normalize_filter_value(value: Any) -> str:
    return str(value).strip()


def _build_fts_query(query: str) -> str:
    tokens = [token for token in re.findall(r"\b[a-z0-9]+\b", query.lower()) if len(token) > 1]
    if not tokens:
        return ""
    unique: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        if token in seen:
            continue
        seen.add(token)
        unique.append(token)
        if len(unique) >= 12:
            break
    return " ".join(unique)


def search_chunks_fts(query: str, limit: int = 12, filters: dict | None = None) -> list[dict[str, Any]]:
    fts_query = _build_fts_query(query)
    if not fts_query:
        return []

    where_parts = ["d.status = 'active'"]
    params: list[Any] = [fts_query]

    filters = filters or {}
    for key in ("department", "application", "status", "file_type", "source_path", "document_id"):
        value = filters.get(key)
        if value is None or value == "":
            continue
        if key == "status":
            where_parts.append(f"d.status = ?")
        elif key == "source_path":
            where_parts.append("d.source_path = ?")
        elif key == "document_id":
            where_parts.append("d.document_id = ?")
        else:
            where_parts.append(f"d.{key} = ?")
        params.append(_normalize_filter_value(value))

    sql = f"""
        SELECT
            c.chunk_id,
            c.document_id,
            c.chunk_index,
            c.page_number,
            c.section_title,
            c.parent_section_title,
            c.chunk_type,
            c.quality_flags,
            c.extraction_method,
            c.has_text_layer,
             c.ocr_confidence,
             c.char_count,
             c.word_count,
             c.workbook_name,
             c.sheet_name,
             c.range_ref,
             c.row_start,
             c.row_end,
             c.col_start,
             c.col_end,
             c.headers,
             c.slide_number,
             c.slide_title,
             c.block_index,
             c.block_type,
             c.shape_name,
             c.shape_id,
             c.has_speaker_notes,
             c.has_structured_preview,
             c.text,
             c.token_count,
             c.source_path,
            c.created_at,
            d.title,
            d.file_name,
            d.file_type,
            d.file_size,
            d.file_hash,
            d.source_fingerprint,
            d.department,
            d.application,
            d.status,
            d.audience,
            d.is_admin_only,
            d.effective_date,
            d.expiration_date,
            d.last_modified,
            d.last_ingested_at,
            d.page_count,
            bm25(chunk_fts) AS rank
        FROM chunk_fts
        JOIN chunks c ON c.chunk_id = chunk_fts.chunk_id
        JOIN documents d ON d.document_id = c.document_id
        WHERE chunk_fts MATCH ?
          AND {" AND ".join(where_parts)}
        ORDER BY rank ASC
        LIMIT ?
    """
    params.append(int(limit))

    with get_connection() as conn:
        rows = conn.execute(sql, params).fetchall()

    results: list[dict[str, Any]] = []
    for row in rows:
        rank = float(row["rank"] or 0.0)
        score = round(1.0 / (1.0 + max(rank, 0.0)), 4)
        results.append({
            "chunk_id": row["chunk_id"],
            "document_id": row["document_id"],
            "chunk_index": row["chunk_index"],
            "page_number": row["page_number"],
            "section_title": row["section_title"],
            "parent_section_title": row["parent_section_title"],
            "chunk_type": row["chunk_type"] or "body",
            "quality_flags": row["quality_flags"],
            "extraction_method": row["extraction_method"],
            "has_text_layer": int(row["has_text_layer"] or 0) if row["has_text_layer"] is not None else None,
            "ocr_confidence": row["ocr_confidence"],
            "char_count": int(row["char_count"] or 0) if row["char_count"] is not None else None,
            "word_count": int(row["word_count"] or 0) if row["word_count"] is not None else None,
            "workbook_name": row["workbook_name"],
            "sheet_name": row["sheet_name"],
            "range_ref": row["range_ref"],
            "row_start": int(row["row_start"] or 0) if row["row_start"] is not None else None,
            "row_end": int(row["row_end"] or 0) if row["row_end"] is not None else None,
            "col_start": int(row["col_start"] or 0) if row["col_start"] is not None else None,
            "col_end": int(row["col_end"] or 0) if row["col_end"] is not None else None,
            "headers": row["headers"],
            "slide_number": int(row["slide_number"] or 0) if row["slide_number"] is not None else None,
            "slide_title": row["slide_title"],
            "block_index": int(row["block_index"] or 0) if row["block_index"] is not None else None,
            "block_type": row["block_type"],
            "shape_name": row["shape_name"],
            "shape_id": row["shape_id"],
            "has_speaker_notes": int(row["has_speaker_notes"] or 0) if row["has_speaker_notes"] is not None else None,
            "has_structured_preview": int(row["has_structured_preview"] or 0) if row["has_structured_preview"] is not None else None,
            "text": row["text"],
            "source": row["title"] or row["file_name"] or row["source_path"],
            "source_name": row["title"] or row["file_name"] or row["source_path"],
            "source_title": row["title"] or row["file_name"] or row["source_path"],
            "source_path": row["source_path"],
            "source_fingerprint": row["source_fingerprint"],
            "last_modified": row["last_modified"],
            "file_size": row["file_size"],
            "department": row["department"],
            "application": row["application"],
            "file_type": row["file_type"],
            "status": row["status"],
            "score": score,
        })
    return results


def start_ingestion_run() -> str:
    run_id = uuid4().hex
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO ingestion_runs (
              run_id, started_at, status, files_seen, files_ingested, files_skipped,
              files_failed, chunks_created, vector_chunks_created, vector_skipped_count, error
            ) VALUES (?, ?, ?, 0, 0, 0, 0, 0, 0, 0, NULL)
            """,
            (run_id, _now(), "running"),
        )
        conn.commit()
    return run_id


def finish_ingestion_run(
    run_id: str,
    *,
    status: str = "completed",
    files_seen: int | None = None,
    files_ingested: int | None = None,
    files_skipped: int | None = None,
    files_failed: int | None = None,
    chunks_created: int | None = None,
    vector_chunks_created: int | None = None,
    vector_skipped_count: int | None = None,
    error: str | None = None,
) -> dict[str, Any] | None:
    updates: dict[str, Any] = {
        "finished_at": _now(),
        "status": status,
        "error": error,
    }
    if files_seen is not None:
        updates["files_seen"] = int(files_seen)
    if files_ingested is not None:
        updates["files_ingested"] = int(files_ingested)
    if files_skipped is not None:
        updates["files_skipped"] = int(files_skipped)
    if files_failed is not None:
        updates["files_failed"] = int(files_failed)
    if chunks_created is not None:
        updates["chunks_created"] = int(chunks_created)
    if vector_chunks_created is not None:
        updates["vector_chunks_created"] = int(vector_chunks_created)
    if vector_skipped_count is not None:
        updates["vector_skipped_count"] = int(vector_skipped_count)

    set_clause = ", ".join(f"{key} = ?" for key in updates)
    params = list(updates.values()) + [run_id]

    with get_connection() as conn:
        conn.execute(
            f"UPDATE ingestion_runs SET {set_clause} WHERE run_id = ?",
            params,
        )
        row = conn.execute(
            "SELECT * FROM ingestion_runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        conn.commit()
    return _row_to_dict(row)


def log_query(entry: dict, user_id: str | None = None) -> dict[str, Any]:
    query_id = entry.get("query_id") or uuid4().hex
    created_at = entry.get("timestamp") or _now()
    sources = entry.get("source_detail") or entry.get("sources") or []
    sources_json = json.dumps(sources, ensure_ascii=False)
    feedback = entry.get("feedback")
    question = entry.get("question") or ""
    answer = entry.get("answer") or ""
    latency_ms = int(entry.get("trace", {}).get("latency_ms") or entry.get("latency_ms") or 0) or None
    with get_connection() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO query_logs (
              query_id, user_id, question, answer, sources_json, latency_ms, created_at, feedback
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (query_id, user_id, question, answer, sources_json, latency_ms, created_at, feedback),
        )
        conn.commit()
    payload = dict(entry)
    payload["query_id"] = query_id
    return payload


def update_query_feedback(user_id: str, timestamp: str, feedback: str) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE query_logs
            SET feedback = ?
            WHERE user_id = ? AND created_at = ?
            """,
            (feedback, user_id, timestamp),
        )
        conn.commit()


def get_recent_query_logs(limit: int = 200) -> list[dict[str, Any]]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT query_id, user_id, question, answer, sources_json, latency_ms, created_at, feedback
            FROM query_logs
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()

    logs: list[dict[str, Any]] = []
    for row in rows:
        sources = []
        raw_sources = row["sources_json"] or "[]"
        try:
            sources = json.loads(raw_sources)
        except json.JSONDecodeError:
            sources = []
        logs.append({
            "query_id": row["query_id"],
            "user_id": row["user_id"],
            "question": row["question"],
            "answer": row["answer"],
            "sources": sources,
            "sources_json": row["sources_json"],
            "latency_ms": row["latency_ms"],
            "created_at": row["created_at"],
            "timestamp": row["created_at"],
            "feedback": row["feedback"],
        })
    return logs


def get_document_stats() -> dict[str, Any]:
    with get_connection() as conn:
        doc_row = conn.execute(
            """
            SELECT
              COUNT(*) AS total_documents,
              SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) AS active_documents,
              SUM(CASE WHEN status != 'active' THEN 1 ELSE 0 END) AS inactive_documents
            FROM documents
            """
        ).fetchone()
        chunk_row = conn.execute(
            """
            SELECT 
                COUNT(*) AS total_chunks,
                SUM(CASE WHEN vector_eligible = 1 THEN 1 ELSE 0 END) AS total_vector_eligible_chunks
            FROM chunks
            """
        ).fetchone()
        last_run = conn.execute(
            "SELECT * FROM ingestion_runs ORDER BY started_at DESC LIMIT 1"
        ).fetchone()

    return {
        "total_documents": int(doc_row["total_documents"] or 0) if doc_row else 0,
        "active_documents": int(doc_row["active_documents"] or 0) if doc_row else 0,
        "inactive_documents": int(doc_row["inactive_documents"] or 0) if doc_row else 0,
        "total_chunks": int(chunk_row["total_chunks"] or 0) if chunk_row else 0,
        "total_vector_eligible_chunks": int(chunk_row["total_vector_eligible_chunks"] or 0) if chunk_row else 0,
        "last_ingestion_run": _row_to_dict(last_run),
    }


def get_index_health(vector_chunks: int | None = None) -> dict[str, Any]:
    active_config = get_active_embedding_config()
    drift_reasons: list[str] = []

    with get_connection() as conn:
        doc_stats = conn.execute(
            """
            SELECT
              COUNT(*) AS total_documents,
              SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) AS active_documents,
              SUM(CASE WHEN status != 'active' THEN 1 ELSE 0 END) AS inactive_documents
            FROM documents
            """
        ).fetchone()
        chunk_stats = conn.execute(
            """
            SELECT
              COUNT(*) AS total_chunks,
              SUM(CASE WHEN d.status = 'active' THEN 1 ELSE 0 END) AS active_chunks,
              SUM(CASE WHEN c.vector_eligible = 1 AND d.status = 'active' THEN 1 ELSE 0 END) AS eligible_chunks,
              SUM(CASE WHEN c.vector_eligible = 0 AND d.status = 'active' THEN 1 ELSE 0 END) AS skipped_chunks
            FROM chunks c
            JOIN documents d ON d.document_id = c.document_id
            """
        ).fetchone()
        
        skip_reason_rows = conn.execute(
            """
            SELECT vector_skip_reason, COUNT(*) as count
            FROM chunks c
            JOIN documents d ON d.document_id = c.document_id
            WHERE d.status = 'active' AND c.vector_eligible = 0
            GROUP BY vector_skip_reason
            """
        ).fetchall()
        
        embedding_model_rows = conn.execute(
            """
            SELECT embedding_model_id, COUNT(*) as count
            FROM chunks c
            JOIN documents d ON d.document_id = c.document_id
            WHERE d.status = 'active' AND c.vector_eligible = 1
            GROUP BY embedding_model_id
            """
        ).fetchall()

        page_method_stats = conn.execute(
            """
            SELECT
              COUNT(*) AS total_pages,
              SUM(CASE WHEN extraction_method = 'text_layer' THEN 1 ELSE 0 END) AS text_layer_pages,
              SUM(CASE WHEN extraction_method = 'ocr' THEN 1 ELSE 0 END) AS ocr_pages,
              SUM(CASE WHEN extraction_method = 'vision_fallback' THEN 1 ELSE 0 END) AS vision_fallback_pages,
              SUM(CASE WHEN extraction_method = 'ocr_failed' THEN 1 ELSE 0 END) AS failed_pages
            FROM document_pages
            """
        ).fetchone()
        chunk_method_stats = conn.execute(
            """
            SELECT
              SUM(CASE WHEN extraction_method = 'ocr' THEN 1 ELSE 0 END) AS ocr_chunk_count,
              SUM(CASE WHEN extraction_method = 'text_layer' THEN 1 ELSE 0 END) AS text_layer_chunk_count,
              SUM(CASE WHEN extraction_method = 'ocr_failed' THEN 1 ELSE 0 END) AS failed_chunk_count,
              SUM(CASE WHEN extraction_method = 'vision_fallback' THEN 1 ELSE 0 END) AS vision_fallback_chunk_count
            FROM chunks
            """
        ).fetchone()
        ocr_document_rows = conn.execute(
            """
            SELECT COUNT(DISTINCT document_id) AS ocr_documents
            FROM document_pages
            WHERE extraction_method = 'ocr'
            """
        ).fetchone()
        ocr_failed_document_rows = conn.execute(
            """
            SELECT COUNT(*) AS ocr_failed_documents
            FROM (
              SELECT document_id
              FROM document_pages
              WHERE extraction_method = 'ocr_failed'
              GROUP BY document_id
              HAVING COUNT(*) > 0
            )
            """
        ).fetchone()
        zero_text_documents_list_rows = conn.execute(
            """
            SELECT d.document_id, d.source_path, d.file_name, d.title
            FROM documents d
            JOIN (
              SELECT document_id
              FROM document_pages
              GROUP BY document_id
              HAVING COALESCE(SUM(char_count), 0) = 0
            ) z ON z.document_id = d.document_id
            """
        ).fetchall()
        zero_text_rows = {"zero_text_documents": len(zero_text_documents_list_rows)}
        extraction_method_rows = conn.execute(
            """
            SELECT
              extraction_method,
              COUNT(*) AS count
            FROM document_pages
            GROUP BY extraction_method
            """
        ).fetchall()
        active_source_rows = conn.execute(
            "SELECT source_path FROM documents WHERE status = 'active' ORDER BY source_path"
        ).fetchall()
        recent_failed_rows = conn.execute(
            """
            SELECT document_id, source_path, file_name, title, ingestion_status, ingestion_error, last_ingested_at
            FROM documents
            WHERE ingestion_status IN ('failed', 'unsupported')
               OR (status != 'active' AND ingestion_error IS NOT NULL)
            ORDER BY COALESCE(last_ingested_at, '') DESC, source_path ASC
            LIMIT 20
            """
        ).fetchall()
        zero_chunk_rows = conn.execute(
            """
            SELECT
              d.document_id,
              d.source_path,
              d.file_name,
              d.title,
              d.file_type,
              d.status,
              d.ingestion_status,
              d.ingestion_error,
              COUNT(c.chunk_id) AS chunks
            FROM documents d
            LEFT JOIN chunks c ON c.document_id = d.document_id
            WHERE d.status = 'active'
              AND d.ingestion_status NOT IN ('unsupported', 'skipped', 'purged', 'not_indexed')
              AND d.file_type NOT IN ('ppt', 'pptx', 'xls', 'ods')
            GROUP BY d.document_id
            HAVING COUNT(c.chunk_id) = 0
            ORDER BY d.source_path ASC
            """
        ).fetchall()
        last_run = conn.execute(
            "SELECT * FROM ingestion_runs ORDER BY started_at DESC LIMIT 1"
        ).fetchone()

        metadata_health = conn.execute(
            """
            SELECT
                SUM(
                    CASE
                        WHEN embedding_model_id IS NULL
                          OR embedding_dimension IS NULL
                          OR embedding_config_hash IS NULL
                          OR embedding_normalized IS NULL
                        THEN 1 ELSE 0
                    END
                ) AS missing_metadata,
                SUM(
                    CASE
                        WHEN embedding_config_hash IS NULL
                        THEN 1 ELSE 0
                    END
                ) AS config_hash_missing,
                SUM(
                    CASE
                        WHEN embedding_model_id IS NOT NULL
                          AND embedding_model_id != ?
                        THEN 1 ELSE 0
                    END
                ) AS model_mismatch,
                SUM(
                    CASE
                        WHEN embedding_config_hash IS NOT NULL
                          AND embedding_config_hash != ?
                        THEN 1 ELSE 0
                    END
                ) AS config_mismatch,
                SUM(
                    CASE
                        WHEN embedding_normalized IS NOT NULL
                          AND embedding_normalized != ?
                        THEN 1 ELSE 0
                    END
                ) AS norm_mismatch
            FROM chunks c
            JOIN documents d ON d.document_id = c.document_id
            WHERE d.status = 'active' AND c.vector_eligible = 1
            """,
            (
                active_config["embedding_model_id"],
                active_config["embedding_config_hash"],
                active_config["embedding_normalized"],
            ),
        ).fetchone()

        dimension_rows = conn.execute(
            """
            SELECT embedding_dimension, COUNT(*) as count
            FROM chunks c
            JOIN documents d ON d.document_id = c.document_id
            WHERE d.status = 'active' AND c.vector_eligible = 1 AND embedding_dimension IS NOT NULL
            GROUP BY embedding_dimension
            """
        ).fetchall()

    total_chunks = int(chunk_stats["total_chunks"] or 0) if chunk_stats else 0
    active_chunks = int(chunk_stats["active_chunks"] or 0) if chunk_stats else 0
    chunk_method_counts = {
        "ocr": int(chunk_method_stats["ocr_chunk_count"] or 0) if chunk_method_stats else 0,
        "text_layer": int(chunk_method_stats["text_layer_chunk_count"] or 0) if chunk_method_stats else 0,
        "ocr_failed": int(chunk_method_stats["failed_chunk_count"] or 0) if chunk_method_stats else 0,
        "vision_fallback": int(chunk_method_stats["vision_fallback_chunk_count"] or 0) if chunk_method_stats else 0,
    }
    extraction_method_counts = {
        str(row["extraction_method"] or "unknown"): int(row["count"] or 0)
        for row in extraction_method_rows
    }

    eligible_chunks = int(chunk_stats["eligible_chunks"] or 0) if chunk_stats else 0

    if vector_chunks is not None and int(vector_chunks) != eligible_chunks:
        drift_reasons.append("vector_count_drift")

    expected_dimension: int | None = None
    try:
        warmup = call_embedding_api(["Spark embedding health warmup"], is_query=True)
        if warmup and warmup[0]:
            expected_dimension = len(warmup[0])
    except Exception:
        expected_dimension = None

    dimension_distribution = {
        str(row["embedding_dimension"]): int(row["count"] or 0)
        for row in dimension_rows
        if row["embedding_dimension"] is not None
    }
    if expected_dimension is None and dimension_distribution:
        expected_dimension = int(
            max(dimension_distribution.items(), key=lambda item: item[1])[0]
        )

    dimension_mismatch_count = 0
    if expected_dimension is not None:
        for dim, count in dimension_distribution.items():
            if int(dim) != int(expected_dimension):
                dimension_mismatch_count += int(count)
    elif len(dimension_distribution) > 1:
        dimension_mismatch_count = sum(int(v) for v in dimension_distribution.values())

    missing_embedding_metadata_count = int(metadata_health["missing_metadata"] or 0) if metadata_health else 0
    model_mismatch_count = int(metadata_health["model_mismatch"] or 0) if metadata_health else 0
    config_hash_missing_count = int(metadata_health["config_hash_missing"] or 0) if metadata_health else 0
    config_mismatch_count = int(metadata_health["config_mismatch"] or 0) if metadata_health else 0
    normalization_mismatch_count = int(metadata_health["norm_mismatch"] or 0) if metadata_health else 0

    if metadata_health:
        if missing_embedding_metadata_count > 0:
            drift_reasons.append("missing_embedding_metadata")
        if model_mismatch_count > 0:
            drift_reasons.append("embedding_model_changed")
        if config_mismatch_count > 0 and "embedding_model_changed" not in drift_reasons:
            drift_reasons.append("embedding_config_changed")
        if normalization_mismatch_count > 0:
            drift_reasons.append("embedding_normalization_changed")

    if dimension_mismatch_count > 0 or len(dimension_distribution) > 1:
        drift_reasons.append("embedding_dimension_changed")

    recommendation = "Index looks healthy."
    if missing_embedding_metadata_count > 0:
        recommendation = "Reindex recommended: missing embedding metadata."
    elif normalization_mismatch_count > 0:
        recommendation = "Reindex recommended: embedding normalization configuration changed."
    elif config_mismatch_count > 0:
        recommendation = "Reindex recommended: embedding configuration changed."
    elif "vector_count_drift" in drift_reasons:
        recommendation = "Reindex recommended: vector count drift."

    total_chunks = int(chunk_stats["total_chunks"] or 0) if chunk_stats else 0
    active_chunks = int(chunk_stats["active_chunks"] or 0) if chunk_stats else 0
    
    return {
        "sqlite_documents": int(doc_stats["total_documents"] or 0) if doc_stats else 0,
        "sqlite_active_documents": int(doc_stats["active_documents"] or 0) if doc_stats else 0,
        "sqlite_inactive_documents": int(doc_stats["inactive_documents"] or 0) if doc_stats else 0,
        "sqlite_chunks": total_chunks,
        "sqlite_active_chunks": active_chunks,
        "sqlite_vector_eligible_chunks": eligible_chunks,
        "sqlite_vector_skipped_chunks": int(chunk_stats["skipped_chunks"] or 0) if chunk_stats else 0,
        "vector_skip_reason_counts": {str(row["vector_skip_reason"] or "unknown"): int(row["count"]) for row in skip_reason_rows},
        "embedding_model_ids": {str(row["embedding_model_id"] or "unknown"): int(row["count"]) for row in embedding_model_rows},
        "embedding_dimensions": {str(row["embedding_dimension"] or "unknown"): int(row["count"]) for row in dimension_rows},
        "ocr_documents": int(ocr_document_rows["ocr_documents"] or 0) if ocr_document_rows else 0,
        "ocr_failed_documents": int(ocr_failed_document_rows["ocr_failed_documents"] or 0) if ocr_failed_document_rows else 0,
        "zero_text_documents": int(zero_text_rows["zero_text_documents"] or 0) if zero_text_rows else 0,
        "zero_text_documents_list": [dict(row) for row in zero_text_documents_list_rows],
        "ocrChunkCount": int(chunk_method_stats["ocr_chunk_count"] or 0) if chunk_method_stats else 0,
        "ocrDocumentCount": int(ocr_document_rows["ocr_documents"] or 0) if ocr_document_rows else 0,
        "ocrPageCount": int(page_method_stats["ocr_pages"] or 0) if page_method_stats else 0,
        "ocrFailedPageCount": int(page_method_stats["failed_pages"] or 0) if page_method_stats else 0,
        "extractionMethodCounts": {str(row["extraction_method"] or "unknown"): int(row["count"]) for row in extraction_method_rows},
        "page_extraction_counts": {
            "total_pages": int(page_method_stats["total_pages"] or 0) if page_method_stats else 0,
            "text_layer_pages": int(page_method_stats["text_layer_pages"] or 0) if page_method_stats else 0,
            "ocr_pages": int(page_method_stats["ocr_pages"] or 0) if page_method_stats else 0,
            "vision_fallback_pages": int(page_method_stats["vision_fallback_pages"] or 0) if page_method_stats else 0,
            "failed_pages": int(page_method_stats["failed_pages"] or 0) if page_method_stats else 0,
        },
        "vector_chunks": vector_chunks,
        "active_source_paths": [str(row["source_path"]) for row in active_source_rows if row["source_path"]],
        "last_ingestion_run": _row_to_dict(last_run),
        "recent_failed_documents": [dict(row) for row in recent_failed_rows],
        "documents_with_zero_chunks": [dict(row) for row in zero_chunk_rows],
        "drift_detected": len(drift_reasons) > 0,
        "drift_reasons": drift_reasons,
        "active_embedding_provider": active_config.get("embedding_provider"),
        "active_embedding_base_url": active_config.get("embedding_base_url"),
        "active_embedding_model_id": active_config.get("embedding_model_id"),
        "active_embedding_normalized": active_config.get("embedding_normalized"),
        "active_embedding_instruction": active_config.get("embedding_instruction"),
        "active_embedding_config_hash": active_config.get("embedding_config_hash"),
        "missing_embedding_metadata_count": missing_embedding_metadata_count,
        "model_mismatch_count": model_mismatch_count,
        "config_hash_missing_count": config_hash_missing_count,
        "config_mismatch_count": config_mismatch_count,
        "normalization_mismatch_count": normalization_mismatch_count,
        "dimension_mismatch_count": dimension_mismatch_count,
        "dimension_distribution": dimension_distribution,
        "expected_embedding_dimension": expected_dimension,
        "recommendation": recommendation,
        "active_embedding_config": active_config,
    }
