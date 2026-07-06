"""Deterministic local-model inference (no network, no GPU, no surprises).

A pack declares model files (path + sha256, verified at startup). A model
file is JSON:

    { "kind": "bow-embed", "dim": N,
      "vocab":     { token: [N floats], ... },
      "buckets":   [ [N floats], ... ],          # optional OOV hash rows
      "centroids": { label: [N floats], ... } }  # optional, for classify

Inference rules make the SAME input + SAME weights produce the SAME output
forever: lowercase \\w+ tokenization, vocab lookup with sha256-hash bucket
fallback, sum + L2 normalization, cosine scoring, ties broken by
lexicographic label. Outputs are CLAIMS: they may shape what agents see
(retrieval, dedup proposals, triage priors) — they never gate a
transition. That rule is structural: model.classify has a single 'ok'
outcome, so a workflow cannot dispatch on a label.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
from pathlib import Path

_TOKEN_RE = re.compile(r"[a-z0-9_]+")

_CACHE = {}


def load_model(path, expected_sha=None):
    """Load (and cache) a weight file, pinned by content hash. Returns
    (weights_dict, model_sha). A sha mismatch is a startup error at pack
    load; here it guards direct callers too."""
    path = Path(path)
    data = path.read_bytes()
    model_sha = hashlib.sha256(data).hexdigest()
    if expected_sha and model_sha != expected_sha:
        raise ValueError("model %s sha256 %s != declared %s"
                         % (path, model_sha, expected_sha))
    if model_sha not in _CACHE:
        weights = json.loads(data.decode("utf-8"))
        if not isinstance(weights.get("dim"), int) or weights["dim"] < 1:
            raise ValueError("model %s: missing positive integer 'dim'" % path)
        _CACHE[model_sha] = weights
    return _CACHE[model_sha], model_sha


def tokenize(text):
    return _TOKEN_RE.findall((text or "").lower())


def embed(text, weights):
    """Sum of token vectors (vocab, then hash-bucket fallback), L2
    normalized. Unknown-token-only input yields the zero vector rather
    than an error — an empty claim, not a failure."""
    dim = weights["dim"]
    vocab = weights.get("vocab", {})
    buckets = weights.get("buckets") or []
    acc = [0.0] * dim
    for tok in tokenize(text):
        row = vocab.get(tok)
        if row is None and buckets:
            idx = int(hashlib.sha256(tok.encode("utf-8")).hexdigest(), 16) % len(buckets)
            row = buckets[idx]
        if row is None:
            continue
        for i in range(dim):
            acc[i] += row[i]
    norm = math.sqrt(sum(x * x for x in acc))
    if norm == 0.0:
        return acc
    return [x / norm for x in acc]


def cosine(a, b):
    num = sum(x * y for x, y in zip(a, b))
    da = math.sqrt(sum(x * x for x in a))
    db = math.sqrt(sum(x * x for x in b))
    if da == 0.0 or db == 0.0:
        return 0.0
    return num / (da * db)


def classify(text, weights):
    """Nearest centroid by cosine; ties break lexicographically by label
    (stable forever). Returns (label, score, margin)."""
    centroids = weights.get("centroids")
    if not centroids:
        raise ValueError("model has no centroids — not a classifier")
    vec = embed(text, weights)
    scored = sorted(((label, cosine(vec, row))
                     for label, row in centroids.items()),
                    key=lambda kv: (-kv[1], kv[0]))
    best_label, best_score = scored[0]
    margin = best_score - scored[1][1] if len(scored) > 1 else best_score
    return best_label, best_score, margin
