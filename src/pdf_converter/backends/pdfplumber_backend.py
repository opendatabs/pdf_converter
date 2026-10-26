"""Markdown and text backend: pdfplumber.

Extra: ``pdfplumber``. Markdown output infers headings from relative font size
and renders extracted tables as markdown tables.
"""

from pathlib import Path

from pdf_converter.backends.base import require, warn_unused_options

NAME = "pdfplumber"
EXTRA = "pdfplumber"

_HEADING_SIZE_RATIO = 1.2
_DEFAULT_FONT_SIZE = 12


def _render_table(table: list[list[str | None]]) -> list[str]:
    """Render one extracted table as markdown lines."""
    if not table or not table[0]:
        return []
    header = [cell or "" for cell in table[0]]
    lines = ["\n| " + " | ".join(header) + " |\n", "|" + " --- |" * len(header)]
    for row in table[1:]:
        lines.append("| " + " | ".join(cell or "" for cell in row) + " |")
    return lines


def to_markdown(input_file: Path, **options) -> str:
    """Extract text with headings and tables from a PDF while keeping structure.

    Args:
        input_file: PDF to convert.
        **options: Ignored.

    Returns:
        Markdown content.
    """
    warn_unused_options(NAME, options)
    pdfplumber = require("pdfplumber", backend=NAME, extra=EXTRA)

    structured_text: list[str] = []
    with pdfplumber.open(input_file) as pdf:
        for page in pdf.pages:
            char_data = page.objects.get("char", [])
            font_sizes = [char["size"] for char in char_data if "size" in char]
            avg_font_size = sum(font_sizes) / len(font_sizes) if font_sizes else _DEFAULT_FONT_SIZE
            size_by_text: dict[str, float] = {}
            for char in char_data:
                if "size" in char and char["text"] not in size_by_text:
                    size_by_text[char["text"]] = char["size"]

            for word in page.extract_words():
                text = word["text"]
                word_font_size = size_by_text.get(text, avg_font_size)
                if word_font_size > avg_font_size * _HEADING_SIZE_RATIO:
                    structured_text.append(f"\n# {text}\n")
                else:
                    structured_text.append(text)

            for table in page.extract_tables():
                structured_text.extend(_render_table(table))

            structured_text.append("\n---\n")
    return "\n".join(structured_text)


def to_text(input_file: Path) -> str:
    """Extract plain text from a PDF, one page after another.

    Args:
        input_file: PDF to read.

    Returns:
        The document text.
    """
    pdfplumber = require("pdfplumber", backend=NAME, extra=EXTRA)

    with pdfplumber.open(input_file) as pdf:
        return "\n".join(page.extract_text() or "" for page in pdf.pages)
