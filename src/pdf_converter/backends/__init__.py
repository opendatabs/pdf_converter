"""Registry of conversion backends.

Each backend lives in its own module and is imported lazily, so selecting one
backend never loads another backend's dependencies. Install only what you need:

===============  ================================  ====================================
Method           Module                            Extra
===============  ================================  ====================================
docling-serve    :mod:`.docling_serve`             ``pdf-converter[docling-serve]``
docling          :mod:`.docling_local`             ``pdf-converter[docling]``
pymupdf4llm      :mod:`.pymupdf4llm_backend`       ``pdf-converter[pymupdf4llm]``
pymupdf          :mod:`.pymupdf_backend`           ``pdf-converter[pymupdf]``
pdfplumber       :mod:`.pdfplumber_backend`        ``pdf-converter[pdfplumber]``
===============  ================================  ====================================
"""

import importlib
import logging
from pathlib import Path
from types import ModuleType

from pdf_converter.backends.base import MissingBackendDependency

logger = logging.getLogger(__name__)

DEFAULT_METHOD = "pymupdf"

MARKDOWN_BACKENDS: dict[str, str] = {
    "docling-serve": "docling_serve",
    "docling": "docling_local",
    "pymupdf4llm": "pymupdf4llm_backend",
    "pymupdf": "pymupdf_backend",
    "pdfplumber": "pdfplumber_backend",
}

TEXT_BACKENDS: dict[str, str] = {
    "pymupdf": "pymupdf_backend",
    "pdfplumber": "pdfplumber_backend",
}

BACKEND_EXTRAS: dict[str, str] = {
    "docling-serve": "docling-serve",
    "docling": "docling",
    "pymupdf4llm": "pymupdf4llm",
    "pymupdf": "pymupdf",
    "pdfplumber": "pdfplumber",
}

REMOTE_BACKENDS = frozenset({"docling-serve"})


def _load(registry: dict[str, str], method: str, kind: str) -> ModuleType:
    """Resolve a method name to its backend module, falling back to the default."""
    name = method.lower()
    module_name = registry.get(name)
    if module_name is None:
        logger.warning(
            "Unknown %s method %r; falling back to %r. Known methods: %s",
            kind,
            method,
            DEFAULT_METHOD,
            ", ".join(sorted(registry)),
        )
        module_name = registry[DEFAULT_METHOD]
    return importlib.import_module(f"{__name__}.{module_name}")


def is_remote(method: str) -> bool:
    """Return True if ``method`` is served by a remote backend."""
    return method.lower() in REMOTE_BACKENDS


def convert_to_markdown(method: str, input_file: Path, **options) -> str:
    """Convert a PDF to markdown with the backend named ``method``.

    Args:
        method: Backend name, e.g. ``"docling-serve"``. Unknown names fall back
            to :data:`DEFAULT_METHOD`.
        input_file: PDF to convert.
        **options: Backend-specific options. Backends that do not understand
            them log a warning.

    Returns:
        Markdown content.

    Raises:
        MissingBackendDependency: If the backend's dependency is not installed.
    """
    return _load(MARKDOWN_BACKENDS, method, "markdown").to_markdown(input_file, **options)


def convert_to_text(method: str, input_file: Path) -> str:
    """Convert a PDF to plain text with the backend named ``method``.

    Args:
        method: Backend name, e.g. ``"pdfplumber"``. Unknown names fall back to
            :data:`DEFAULT_METHOD`.
        input_file: PDF to read.

    Returns:
        The document text.

    Raises:
        MissingBackendDependency: If the backend's dependency is not installed.
    """
    return _load(TEXT_BACKENDS, method, "text").to_text(input_file)


__all__ = [
    "BACKEND_EXTRAS",
    "DEFAULT_METHOD",
    "MARKDOWN_BACKENDS",
    "REMOTE_BACKENDS",
    "TEXT_BACKENDS",
    "MissingBackendDependency",
    "convert_to_markdown",
    "convert_to_text",
    "is_remote",
]
