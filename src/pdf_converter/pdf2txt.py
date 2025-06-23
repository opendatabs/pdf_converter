import tempfile
from pathlib import Path

import fitz  # PyMuPDF
import pdfplumber


class TextConverter:
    def __init__(self, lib: str, input_file: Path):
        self.lib = lib.lower()
        self.input_file = input_file
        fd, temp_path = tempfile.mkstemp(suffix=".txt")
        self.output_file = Path(temp_path)
        self.txt_content = ""

    def pymupdf_text(self):
        doc = fitz.open(self.input_file)
        return "\n".join(page.get_text() for page in doc)

    def pdfplumber_text(self):
        output_lines = []
        with pdfplumber.open(self.input_file) as pdf:
            for page in pdf.pages:
                output_lines.append(page.extract_text() or "")
        return "\n".join(output_lines)

    def convert(self):
        if self.lib == "pdfplumber":
            self.txt_content = self.pdfplumber_text()
        else:
            self.txt_content = self.pymupdf_text()
        with open(self.output_file, "w", encoding="utf-8") as f:
            f.write(self.txt_content)
