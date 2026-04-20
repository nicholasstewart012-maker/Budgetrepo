from __future__ import annotations

import hashlib
import os
import re
import sys
import time
from collections import deque
from html import unescape
from pathlib import Path
from urllib.parse import urljoin, urlparse, urldefrag

import requests


START_URL = "https://www.renasantbank.com/"
ALLOWED_HOSTS = {"www.renasantbank.com", "renasantbank.com"}
OUTPUT_DIR = Path("intake/renasantbank_pdfs")
MAX_PAGES = 250
REQUEST_TIMEOUT = 20
USER_AGENT = "Mozilla/5.0 (compatible; PDFScraper/1.0; +https://www.renasantbank.com/)"


HREF_RE = re.compile(r"(?:href|src)=[\"']([^\"'#>]+)[\"']", re.IGNORECASE)


def normalize_url(base: str, link: str) -> str | None:
    if not link:
        return None
    link = unescape(link.strip())
    if link.startswith(("mailto:", "tel:", "javascript:")):
        return None
    absolute = urljoin(base, link)
    absolute, _ = urldefrag(absolute)
    parsed = urlparse(absolute)
    if parsed.scheme not in {"http", "https"}:
        return None
    return absolute


def is_allowed_page(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.netloc.lower() in ALLOWED_HOSTS and not parsed.path.lower().endswith(".pdf")


def is_pdf_url(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.path.lower().endswith(".pdf")


def safe_filename_from_url(url: str, content: bytes) -> str:
    parsed = urlparse(url)
    name = os.path.basename(parsed.path) or "document.pdf"
    if not name.lower().endswith(".pdf"):
        name += ".pdf"
    stem, ext = os.path.splitext(name)
    digest = hashlib.sha1(content).hexdigest()[:10]
    return f"{stem}_{digest}{ext}"


def looks_like_pdf(content: bytes) -> bool:
    return content.startswith(b"%PDF-")


def fetch(session: requests.Session, url: str) -> requests.Response | None:
    try:
        response = session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        return response
    except requests.RequestException as exc:
        print(f"WARN failed to fetch {url}: {exc}", file=sys.stderr)
        return None


def save_pdf(url: str, content: bytes, downloaded: dict[str, str]) -> bool:
    if not looks_like_pdf(content):
        print(f"WARN skipped non-PDF content from {url}", file=sys.stderr)
        return False

    filename = safe_filename_from_url(url, content)
    out_path = OUTPUT_DIR / filename
    if not out_path.exists():
        out_path.write_bytes(content)
    downloaded[url] = str(out_path)
    print(f"PDF {url} -> {out_path}")
    return True


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    queue = deque([START_URL])
    visited_pages: set[str] = set()
    found_pdfs: set[str] = set()
    downloaded: dict[str, str] = {}

    while queue and len(visited_pages) < MAX_PAGES:
        url = queue.popleft()
        if url in visited_pages:
            continue
        visited_pages.add(url)
        print(f"PAGE {len(visited_pages):03d}: {url}")

        response = fetch(session, url)
        if response is None:
            continue

        content_type = response.headers.get("content-type", "").lower()
        final_url = response.url

        if "pdf" in content_type or is_pdf_url(final_url):
            pdf_url = final_url
            if pdf_url not in found_pdfs:
                found_pdfs.add(pdf_url)
                save_pdf(pdf_url, response.content, downloaded)
            continue

        if "html" not in content_type and "text/" not in content_type and "application/xhtml+xml" not in content_type:
            continue

        html = response.text
        for match in HREF_RE.finditer(html):
            normalized = normalize_url(final_url, match.group(1))
            if not normalized:
                continue
            if is_pdf_url(normalized):
                if normalized not in found_pdfs:
                    found_pdfs.add(normalized)
                    pdf_response = fetch(session, normalized)
                    if pdf_response is None:
                        continue
                    pdf_content_type = pdf_response.headers.get("content-type", "").lower()
                    if "pdf" not in pdf_content_type and not is_pdf_url(pdf_response.url):
                        continue
                    save_pdf(pdf_response.url, pdf_response.content, downloaded)
                    time.sleep(0.15)
            elif is_allowed_page(normalized) and normalized not in visited_pages:
                queue.append(normalized)

        time.sleep(0.15)

    manifest_path = OUTPUT_DIR / "manifest.txt"
    lines = [
        f"Visited pages: {len(visited_pages)}",
        f"Downloaded PDFs: {len(downloaded)}",
        "",
    ]
    for pdf_url, file_path in sorted(downloaded.items()):
        lines.append(f"{pdf_url}\t{file_path}")
    manifest_path.write_text("\n".join(lines), encoding="utf-8")

    print(f"DONE visited={len(visited_pages)} downloaded={len(downloaded)} manifest={manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())