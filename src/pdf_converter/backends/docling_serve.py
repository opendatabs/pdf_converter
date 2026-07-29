"""Markdown backend: remote Docling Serve instance (async submit/poll/result).

Extra: ``docling-serve`` (installs ``httpx``).

Requires ``DOCLING_HTTP_CLIENT`` in the environment; ``DOCLING_API_KEY`` is
optional for unauthenticated internal instances.
"""

from pathlib import Path

from pdf_converter.backends.base import require

NAME = "docling-serve"
EXTRA = "docling-serve"

DEFAULTS = {
    "to_formats": ["md"],
    "image_export_mode": "embedded",
    "pipeline": "standard",
    "do_ocr": True,
    "force_ocr": False,
    "ocr_engine": "easyocr",
    "ocr_lang": ["en", "fr", "de", "it"],
    "pdf_backend": "pypdfium2",
    "table_mode": "accurate",
    "abort_on_error": False,
    "return_as_file": False,
}


def to_markdown(input_file: Path, **options) -> str:
    """Convert a PDF to markdown via the Docling Serve async API.

    Args:
        input_file: PDF to convert.
        **options: Forwarded to
            :func:`pdf_converter.docling_client.convert_file_to_markdown`.

    Returns:
        Markdown content.

    Raises:
        DoclingServeError: If the conversion fails.
    """
    require("httpx", backend=NAME, extra=EXTRA)
    from pdf_converter import docling_client

    return docling_client.convert_file_to_markdown(input_file, **{**DEFAULTS, **options})
