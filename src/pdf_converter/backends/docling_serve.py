"""Markdown backend: remote Docling Serve instance (async submit/poll/result).

Extra: ``docling-serve`` (installs ``httpx``).

Requires ``DOCLING_HTTP_CLIENT`` in the environment; ``DOCLING_API_KEY`` is
optional for unauthenticated internal instances.

Uses ``POST /v1/convert/source/async`` with a local file as base64 by default.
Optional ``source_url`` is only useful when the host is on Docling Serve's
URL allowlist.
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


def to_markdown(input_file: Path | None = None, **options) -> str:
    """Convert a PDF to markdown via the Docling Serve async source API.

    Args:
        input_file: Local PDF uploaded as base64 when ``source_url`` is not set.
        **options: Forwarded to
            :func:`pdf_converter.docling_client.convert_file_to_markdown`.
            ``source_url`` is optional and only works for allowlisted hosts.

    Returns:
        Markdown content.

    Raises:
        DoclingServeError: If the conversion fails.
    """
    require("httpx", backend=NAME, extra=EXTRA)
    from pdf_converter import docling_client

    return docling_client.convert_file_to_markdown(input_file, **{**DEFAULTS, **options})
