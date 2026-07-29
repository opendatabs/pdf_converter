"""Markdown backend: local docling install.

Extra: ``docling``. This is by far the heaviest backend — it pulls in torch and
the docling model stack. Prefer ``docling-serve`` if a server is available.
"""

from pathlib import Path

from pdf_converter.backends.base import require, warn_unused_options

NAME = "docling"
EXTRA = "docling"


def to_markdown(input_file: Path, **options) -> str:
    """Convert a PDF to markdown with a locally running docling pipeline.

    Args:
        input_file: PDF to convert.
        **options: Ignored; local docling is configured through docling itself.

    Returns:
        Markdown content.
    """
    warn_unused_options(NAME, options)
    docling = require("docling.document_converter", backend=NAME, extra=EXTRA)

    converter = docling.DocumentConverter()
    return converter.convert(input_file).document.export_to_markdown()
