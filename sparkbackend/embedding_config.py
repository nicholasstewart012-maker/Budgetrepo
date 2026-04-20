import os
import json
import hashlib
import math
import requests
from pathlib import Path
from dotenv import load_dotenv
from functools import lru_cache
from typing import Any, List

# Load environment
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path, override=True)

# Centralized Embedding Configuration
EMBEDDING_PROVIDER = os.getenv("EMBEDDING_PROVIDER", "ollama")
LIMINAL_URL = os.getenv("LIMINAL_URL", "http://localhost:11434")
EMBEDDING_BASE_URL = os.getenv("EMBEDDING_BASE_URL", LIMINAL_URL)
EMBEDDING_MODEL_ID = os.getenv("EMBEDDING_MODEL_ID", "qwen3-embedding:4b")
EMBEDDING_MODEL_PATH = os.getenv("EMBEDDING_MODEL_PATH", r"C:\Spark\models\bge-m3")
EMBEDDING_LOCAL_BATCH_SIZE = int(os.getenv("EMBEDDING_LOCAL_BATCH_SIZE", "16"))
EMBEDDING_LOCAL_MAX_LENGTH = int(os.getenv("EMBEDDING_LOCAL_MAX_LENGTH", "8192"))
EMBEDDING_LOCAL_USE_FP16 = os.getenv("EMBEDDING_LOCAL_USE_FP16", "false").lower() == "true"
EMBEDDING_LOCAL_DEVICE = os.getenv("EMBEDDING_LOCAL_DEVICE", "").strip()
EMBEDDING_VALIDATE_ON_STARTUP = os.getenv("EMBEDDING_VALIDATE_ON_STARTUP", "true").lower() == "true"
EMBEDDING_NORMALIZE = os.getenv("EMBEDDING_NORMALIZE", "true").lower() == "true"
EMBEDDING_INSTRUCTION = os.getenv("EMBEDDING_INSTRUCTION", "Represent this internal policy/procedure chunk for retrieval by employee questions.")

_active_location = EMBEDDING_BASE_URL if EMBEDDING_PROVIDER == "ollama" else EMBEDDING_MODEL_PATH
print(
    "[Embedding Config] Active embeddings: "
    f"provider={EMBEDDING_PROVIDER} model_id={EMBEDDING_MODEL_ID} location={_active_location}"
)

def get_embedding_config_hash() -> str:
    """Generates a unique hash for the current embedding configuration."""
    config_data = {
        "provider": EMBEDDING_PROVIDER,
        "model_id": EMBEDDING_MODEL_ID,
        "model_path": EMBEDDING_MODEL_PATH if EMBEDDING_PROVIDER != "ollama" else "",
        "normalize": EMBEDDING_NORMALIZE,
        "instruction": EMBEDDING_INSTRUCTION
    }
    config_json = json.dumps(config_data, sort_keys=True)
    return hashlib.sha256(config_json.encode()).hexdigest()[:16]

def get_active_embedding_config() -> dict:
    """Returns the current active embedding configuration for metadata storage and drift detection."""
    return {
        "embedding_provider": EMBEDDING_PROVIDER,
        "embedding_base_url": EMBEDDING_BASE_URL,
        "embedding_model_id": EMBEDDING_MODEL_ID,
        "embedding_model_path": EMBEDDING_MODEL_PATH if EMBEDDING_PROVIDER != "ollama" else "",
        "embedding_normalized": 1 if EMBEDDING_NORMALIZE else 0,
        "embedding_instruction": EMBEDDING_INSTRUCTION,
        "embedding_config_hash": get_embedding_config_hash(),
    }

def _l2_normalize(vector: list[float]) -> list[float]:
    norm = math.sqrt(sum(float(v) * float(v) for v in vector))
    if norm <= 0:
        return [float(v) for v in vector]
    return [float(v) / norm for v in vector]

def _resolve_local_device() -> str:
    if EMBEDDING_LOCAL_DEVICE:
        return EMBEDDING_LOCAL_DEVICE
    try:
        import torch
        if torch.cuda.is_available():
            return "cuda:0"
    except Exception:
        pass
    return "cpu"

def _apply_instruction(text: str) -> str:
    # Preserve current behavior: a single instruction string is applied to both
    # ingestion and query embeddings (previously sent to the embedding API).
    if not EMBEDDING_INSTRUCTION:
        return text
    return f"{EMBEDDING_INSTRUCTION}\n\n{text}"

@lru_cache(maxsize=1)
def _get_local_bge_m3_model():
    model_path = Path(EMBEDDING_MODEL_PATH)
    config_json = model_path / "config.json"
    if not config_json.exists():
        raise RuntimeError(
            "Local embedding model path is missing or incomplete. "
            f"Expected Hugging Face model files under: {model_path}. "
            "Download with: hf download BAAI/bge-m3 --local-dir "
            f"{model_path}"
        )

    try:
        from FlagEmbedding import BGEM3FlagModel  # type: ignore
    except Exception as exc:
        raise RuntimeError(
            "FlagEmbedding is required for local BGE-M3 embeddings. "
            "Install with: pip install FlagEmbedding==1.3.5 transformers==4.44.2"
        ) from exc

    device = _resolve_local_device()
    use_fp16 = EMBEDDING_LOCAL_USE_FP16 and device.startswith("cuda")
    print(
        "[Embedding Config] Loading local embedding model: "
        f"{EMBEDDING_MODEL_ID} path={model_path} device={device} fp16={use_fp16}"
    )
    try:
        return BGEM3FlagModel(
            str(model_path),
            use_fp16=use_fp16,
            devices=device,
        )
    except Exception as exc:
        raise RuntimeError(
            f"Failed to load local embedding model from {model_path}: {exc}"
        ) from exc

if EMBEDDING_VALIDATE_ON_STARTUP and EMBEDDING_PROVIDER in {"local_bge_m3", "bge-m3", "bge_m3", "flagembedding"}:
    # Fail fast if the local model path is missing or the model cannot be loaded.
    _get_local_bge_m3_model()

def call_embedding_api(texts: List[str], is_query: bool = False) -> List[List[float]]:
    """
    Returns embeddings for a batch of texts.
    - provider=ollama: calls Ollama-compatible /api/embed
    - provider=local_bge_m3: loads BGE-M3 locally via FlagEmbedding (dense only)
    """
    if EMBEDDING_PROVIDER == "ollama":
        url = f"{EMBEDDING_BASE_URL.rstrip('/')}/api/embed"
        embeddings = []
        
        # Ollama /api/embed supports a list of inputs
        payload = {
            "model": EMBEDDING_MODEL_ID,
            "input": texts,
        }
        
        # Add instruction if provided and not empty
        # Note: Ollama's /api/embed might use 'instruction' or we might prepend it to input
        # Qwen3-Embedding-4B often benefits from instructions
        if EMBEDDING_INSTRUCTION:
            payload["instruction"] = EMBEDDING_INSTRUCTION

        try:
            response = requests.post(url, json=payload, timeout=60)
            response.raise_for_status()
            data = response.json()
            
            # Ollama returns 'embeddings' as a list of lists
            embeddings = data.get("embeddings", [])
            
            if not embeddings and "embedding" in data:
                # Some versions might return a single 'embedding' if only one input
                embeddings = [data["embedding"]]

            normalized_embeddings: list[list[float]] = []
            for raw_vector in embeddings:
                vector = [float(v) for v in (raw_vector or [])]
                if EMBEDDING_NORMALIZE:
                    vector = _l2_normalize(vector)
                normalized_embeddings.append(vector)

            if len(normalized_embeddings) != len(texts):
                raise RuntimeError(
                    f"Embedding API returned {len(normalized_embeddings)} vectors for {len(texts)} inputs."
                )

            return normalized_embeddings
        except Exception as e:
            print(f"[Embedding Config] Error calling {EMBEDDING_PROVIDER} API: {e}")
            raise
    elif EMBEDDING_PROVIDER in {"local_bge_m3", "bge-m3", "bge_m3", "flagembedding"}:
        model = _get_local_bge_m3_model()
        prepared = [_apply_instruction(t or "") for t in texts]
        try:
            # FlagEmbedding's BGEM3FlagModel returns a dict with dense_vecs when
            # return_dense=True. We explicitly disable sparse/multi-vector paths.
            encoded = model.encode(
                prepared,
                batch_size=EMBEDDING_LOCAL_BATCH_SIZE,
                max_length=EMBEDDING_LOCAL_MAX_LENGTH,
                return_dense=True,
                return_sparse=False,
                return_colbert_vecs=False,
            )
            dense_vecs = encoded.get("dense_vecs") if isinstance(encoded, dict) else None
            if dense_vecs is None:
                raise RuntimeError("BGEM3FlagModel.encode did not return dense_vecs")
            embeddings = [list(map(float, vec)) for vec in dense_vecs]
        except TypeError:
            # Compatibility fallback for older FlagEmbedding versions where encode()
            # may not accept return_dense/return_sparse/return_colbert_vecs kwargs.
            dense_vecs = model.encode(
                prepared,
                batch_size=EMBEDDING_LOCAL_BATCH_SIZE,
                max_length=EMBEDDING_LOCAL_MAX_LENGTH,
            )
            embeddings = [list(map(float, vec)) for vec in dense_vecs]

        normalized_embeddings: list[list[float]] = []
        for raw_vector in embeddings:
            vector = [float(v) for v in (raw_vector or [])]
            if EMBEDDING_NORMALIZE:
                vector = _l2_normalize(vector)
            normalized_embeddings.append(vector)

        if len(normalized_embeddings) != len(texts):
            raise RuntimeError(
                f"Local embedder returned {len(normalized_embeddings)} vectors for {len(texts)} inputs."
            )
        return normalized_embeddings
    else:
        raise ValueError(f"Unsupported embedding provider: {EMBEDDING_PROVIDER}")

if __name__ == "__main__":
    # Quick test
    print(f"Config Hash: {get_embedding_config_hash()}")
    print(f"Active Config: {get_active_embedding_config()}")
