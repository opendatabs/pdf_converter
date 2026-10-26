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

from pdf_converter.backends.base import (
    EXIT_MISSING_DEPENDENCY,
    EXIT_UNKNOWN_BACKEND,
    MissingBackendDependency,
    UnknownBackend,
)

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


def _resolve(registry: dict[str, str], method: str, kind: str) -> str:
    """Resolve a method name to its backend module name.

    Unknown names raise rather than falling back: output filenames are built
    from the *requested* method, so a silent fallback would store one backend's
    output under another backend's name.
    """
    module_name = registry.get(method.lower())
    if module_name is None:
        hint = ""
        if method.lower() in REMOTE_BACKENDS:
            hint = f" ({method!r} is a remote markdown-only backend.)"
        raise UnknownBackend(
            f"Unknown {kind} method {method!r}.{hint} Known {kind} methods: {', '.join(sorted(registry))}"
        )
    return module_name


def _load(registry: dict[str, str], method: str, kind: str) -> ModuleType:
    """Import the backend module for ``method``."""
    return importlib.import_module(f"{__name__}.{_resolve(registry, method, kind)}")


def is_remote(method: str) -> bool:
    """Return True if ``method`` is served by a remote backend."""
    return method.lower() in REMOTE_BACKENDS


def validate_method(method: str, kind: str = "markdown") -> None:
    """Raise :class:`UnknownBackend` unless ``method`` is a registered backend.

    Args:
        method: Backend name to check.
        kind: ``"markdown"`` or ``"text"``, selecting the registry to check against.

    Raises:
        UnknownBackend: If ``method`` is not registered for ``kind``.
    """
    registry = MARKDOWN_BACKENDS if kind == "markdown" else TEXT_BACKENDS
    _resolve(registry, method, kind)


def convert_to_markdown(method: str, input_file: Path | None = None, **options) -> str:
    """Convert a PDF to markdown with the backend named ``method``.

    Args:
        method: Backend name, e.g. ``"docling-serve"``.
        input_file: PDF to convert. Optional for remote backends that receive a
            ``source_url`` in ``options`` instead.
        **options: Backend-specific options. Backends that do not understand
            them log a warning.

    Returns:
        Markdown content.

    Raises:
        UnknownBackend: If ``method`` is not a known markdown backend.
        MissingBackendDependency: If the backend's dependency is not installed.
    """
    return _load(MARKDOWN_BACKENDS, method, "markdown").to_markdown(input_file, **options)


def convert_to_text(method: str, input_file: Path) -> str:
    """Convert a PDF to plain text with the backend named ``method``.

    Args:
        method: Backend name, e.g. ``"pdfplumber"``.
        input_file: PDF to read.

    Returns:
        The document text.

    Raises:
        UnknownBackend: If ``method`` is not a known text backend.
        MissingBackendDependency: If the backend's dependency is not installed.
    """
    return _load(TEXT_BACKENDS, method, "text").to_text(input_file)


__all__ = [
    "BACKEND_EXTRAS",
    "DEFAULT_METHOD",
    "EXIT_MISSING_DEPENDENCY",
    "EXIT_UNKNOWN_BACKEND",
    "MARKDOWN_BACKENDS",
    "REMOTE_BACKENDS",
    "TEXT_BACKENDS",
    "MissingBackendDependency",
    "UnknownBackend",
    "convert_to_markdown",
    "convert_to_text",
    "is_remote",
    "validate_method",
]
