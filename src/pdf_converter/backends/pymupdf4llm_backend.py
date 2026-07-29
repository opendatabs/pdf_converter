"""Markdown backend: pymupdf4llm.

Extra: ``pymupdf4llm``.
"""

from pathlib import Path

from pdf_converter.backends.base import require, warn_unused_options

NAME = "pymupdf4llm"
EXTRA = "pymupdf4llm"


def to_markdown(input_file: Path, **options) -> str:
    """Convert a PDF to markdown with pymupdf4llm.

    Args:
        input_file: PDF to convert.
        **options: Ignored.

    Returns:
        Markdown content.
    """
    warn_unused_options(NAME, options)
    pymupdf4llm = require("pymupdf4llm", backend=NAME, extra=EXTRA)

    return pymupdf4llm.to_markdown(input_file)
