"""HTTP API (FastAPI) over the built datasets."""

from .server import create_app

__all__ = ["create_app"]
