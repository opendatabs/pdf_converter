"""Markdown and text backend: PyMuPDF (fitz).

Extra: ``pymupdf``. Markdown output uses a font-size heuristic to infer headings;
text output is a plain per-page text dump.
"""

import re
from pathlib import Path
from typing import Any

from pdf_converter.backends.base import require, warn_unused_options

NAME = "pymupdf"
EXTRA = "pymupdf"

_H1_MIN_SIZE = 18
_H2_MIN_SIZE = 16
_H3_MIN_SIZE = 14
_HEADING_MIN_SIZE = 12
_HEADING_MAX_LENGTH = 80
_SENTENCE_ENDINGS = (".", ",", ";", ":", "?", "!")


def _collect_text_blocks(doc: Any) -> list[dict[str, Any]]:
    """Flatten a PyMuPDF document into per-line blocks with formatting hints."""
    text_blocks: list[dict[str, Any]] = []
    for page_num in range(len(doc)):
        page = doc[page_num]
        blocks = page.get_text("dict")["blocks"]

        for block in blocks:
            if "lines" not in block:
                continue
            for line in block["lines"]:
                if "spans" not in line:
                    continue

                line_text = ""
                is_bold = False
                font_size = 0

                for span in line["spans"]:
                    if not span["text"].strip():
                        continue
                    font_size = max(font_size, span["size"])
                    if "bold" in span["font"].lower() or span["flags"] & 2:
                        is_bold = True
                    line_text += span["text"]

                if line_text.strip():
                    text_blocks.append(
                        {
                            "text": line_text.strip(),
                            "is_bold": is_bold,
                            "is_heading": font_size > _HEADING_MIN_SIZE,
                            "font_size": font_size,
                            "page": page_num + 1,
                        }
                    )
    return text_blocks


def _render_markdown(text_blocks: list[dict[str, Any]]) -> str:
    """Render collected text blocks as markdown."""
    md_lines: list[str] = []
    prev_block: dict[str, Any] | None = None

    for block in text_blocks:
        text = block["text"].strip()
        if not text:
            continue

        looks_like_title = len(text) < _HEADING_MAX_LENGTH and not text.endswith(_SENTENCE_ENDINGS)
        if block["is_heading"] or looks_like_title:
            if block["font_size"] >= _H1_MIN_SIZE:
                md_lines.append(f"# {text}")
            elif block["font_size"] >= _H2_MIN_SIZE:
                md_lines.append(f"## {text}")
            elif block["font_size"] >= _H3_MIN_SIZE:
                md_lines.append(f"### {text}")
            elif block["is_bold"]:
                md_lines.append(f"**{text}**")
            else:
                md_lines.append(text)
        elif block["is_bold"]:
            md_lines.append(f"**{text}**")
        else:
            md_lines.append(text)

        if prev_block and prev_block["page"] != block["page"]:
            md_lines.append("\n---\n")
        prev_block = block

    return re.sub(r"\n{3,}", "\n\n", "\n\n".join(md_lines))


def to_markdown(input_file: Path, **options) -> str:
    """Convert a PDF to markdown using PyMuPDF text extraction plus heuristics.

    Args:
        input_file: PDF to convert.
        **options: Ignored.

    Returns:
        Markdown content.
    """
    warn_unused_options(NAME, options)
    fitz = require("fitz", backend=NAME, extra=EXTRA)

    with fitz.open(input_file) as doc:
        return _render_markdown(_collect_text_blocks(doc))


def to_text(input_file: Path) -> str:
    """Extract plain text from a PDF, one page after another.

    Args:
        input_file: PDF to read.

    Returns:
        The document text.
    """
    fitz = require("fitz", backend=NAME, extra=EXTRA)

    with fitz.open(input_file) as doc:
        return "\n".join(page.get_text() for page in doc)
