"""Stand-in embeddings for tests: words hashed into 384 dimensions (no model download)."""

import hashlib
import math


def embed_stub(text: str, dims: int = 384) -> list[float]:
    v = [0.0] * dims
    for w in text.lower().split():
        h = hashlib.sha1(w.encode()).digest()
        v[int.from_bytes(h[:2], "little") % dims] += 1.0 if h[2] & 1 else -1.0
    n = math.sqrt(sum(x * x for x in v)) or 1.0
    return [x / n for x in v]
