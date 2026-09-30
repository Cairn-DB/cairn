"""Creates the `articles` collection (admin key): `CAIRN_ADMIN_KEY=... python setup.py`."""

import os

from cairn_db import Client

SCHEMA = {"fields": [
    {"name": "embedding", "kind": {"Vector": {"dims": 384, "metric": "Cosine"}}},
    {"name": "text", "kind": "Text"},
    {"name": "title", "kind": "Text"},
    {"name": "parent", "kind": "Enum"},
    {"name": "product", "kind": "Enum"},
    {"name": "lang", "kind": "Enum"},
    {"name": "rev", "kind": "I64"},
    {"name": "deprecated", "kind": "Bool"},
]}

if __name__ == "__main__":
    admin = Client(os.environ.get("CAIRN_URL", "http://localhost:7200"), api_key=os.environ["CAIRN_ADMIN_KEY"])
    if "articles" not in [c["name"] for c in admin.list_collections()]:
        admin.create_collection("articles", SCHEMA, shards=4)
    print("ready")
