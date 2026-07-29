"""``TextConverter``: PDF -> plain text, backed by the backend registry."""

import os
import tempfile
from pathlib import Path

from pdf_converter import backends


class TextConverter:
    """Convert a single PDF to plain text with the backend named ``lib``."""

    def __init__(self, lib: str, input_file: Path):
        self.lib = lib.lower()
        self.input_file = input_file
        fd, temp_path = tempfile.mkstemp(suffix=".txt")
        os.close(fd)
        self.output_file = Path(temp_path)
        self.txt_content = ""

    def cleanup(self):
        """Remove the temporary output file."""
        self.output_file.unlink(missing_ok=True)

    def convert(self):
        """Run the conversion and write the result to ``self.output_file``."""
        self.txt_content = backends.convert_to_text(self.lib, self.input_file)
        with open(self.output_file, "w", encoding="utf-8") as f:
            f.write(self.txt_content)
