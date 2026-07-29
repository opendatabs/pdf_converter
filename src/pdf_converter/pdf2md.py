"""``Converter``: PDF -> Markdown, backed by the pluggable backend registry.

The conversion logic itself lives in :mod:`pdf_converter.backends`; this module
owns the temp-file lifecycle, the download-link helpers and the error contract
used by :mod:`pdf_converter.convert_single_pdf2md`.
"""

import base64
import logging
import os
import shutil
import tempfile
import zipfile
from pathlib import Path

from dotenv import load_dotenv

from pdf_converter import backends
from pdf_converter.backends import MissingBackendDependency

load_dotenv()

logger = logging.getLogger(__name__)

IMAGE_FOLDER = Path("./images")


class Converter:
    """Convert a single PDF to Markdown with the backend named ``lib``."""

    def __init__(self, lib: str, input_file: Path):
        self.lib = lib
        self.input_file = input_file
        fd, temp_path = tempfile.mkstemp(suffix=".md")
        os.close(fd)
        self.output_file = Path(temp_path)
        self.doc_image_folder = IMAGE_FOLDER / self.output_file.stem
        self.md_content = ""
        self.create_image_zip_file = False
        self.last_error: str | None = None

    def cleanup(self):
        """Remove the temporary output file and any per-document image folder."""
        self.output_file.unlink(missing_ok=True)
        if self.doc_image_folder.exists():
            shutil.rmtree(self.doc_image_folder, ignore_errors=True)

    def has_image_extraction(self):
        return self.lib.lower() in ["mistral-ocr"]

    def extract_images_from_pdf(self):
        """Write every embedded image of the source PDF into the image folder."""
        from pdf_converter.backends import images

        return images.extract_images(self.input_file, self.doc_image_folder)

    def zip_markdown_doc_with_images(self):
        with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tmp_zip_file:
            temp_zip_path = Path(tmp_zip_file.name)

        with zipfile.ZipFile(temp_zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            if self.output_file.exists():
                zipf.write(self.output_file, self.output_file.name)

            if self.doc_image_folder.exists():
                for root, _, files in os.walk(self.doc_image_folder):
                    for file in files:
                        file_path = Path(root) / file
                        zipf.write(file_path, Path("images") / file)
        return Path(temp_zip_path)

    def get_zipped_images(self):
        shutil.make_archive(str(self.doc_image_folder), "zip", self.doc_image_folder)
        return f"{self.doc_image_folder}.zip"

    def get_file_download_link(self, link_text: str):
        """Generate a download link for an existing file"""
        if self.create_image_zip_file and self.output_file.exists():
            zip_file = self.zip_markdown_doc_with_images()
            with zip_file.open("rb") as f:
                bytes_data = f.read()
            b64 = base64.b64encode(bytes_data).decode()
            mime_type = "application/zip"
            href = f'<a href="data:file/{mime_type};base64,{b64}" download="{zip_file.name}">{link_text}</a>'
            return href
        elif self.output_file.exists():
            with self.output_file.open("rb") as f:
                bytes_data = f.read()
            b64 = base64.b64encode(bytes_data).decode()
            mime_type = "application/pdf" if self.output_file.suffix == ".pdf" else "text/markdown"
            filename = os.path.basename(self.output_file)
            href = f'<a href="data:file/{mime_type};base64,{b64}" download="{filename}">{link_text}</a>'
            return href
        return None

    def convert(self, **options):
        """Run the conversion and write the result to ``self.output_file``.

        Failures are recorded on ``self.last_error`` and produce empty output,
        keeping the subprocess contract stable.

        Args:
            **options: Backend-specific options, merged over the backend's own
                defaults.
        """
        lib = self.lib.lower()
        self.last_error = None
        try:
            self.md_content = backends.convert_to_markdown(lib, self.input_file, **options)
        except MissingBackendDependency:
            raise
        except Exception as e:
            self.last_error = f"Conversion with {lib} failed: {e}"
            logger.error(self.last_error)
            self.md_content = None

        if self.md_content is None:
            if not self.last_error:
                self.last_error = f"Conversion with {lib} returned no content"
            self.md_content = ""

        with open(self.output_file, "w", encoding="utf-8") as f:
            f.write(self.md_content)
