import base64
import io
import json
import logging
import os
import re
import shutil
import tempfile
import zipfile
from pathlib import Path

import fitz  # PyMuPDF
import httpx
import pdfplumber
import pymupdf4llm
import requests
from docling.document_converter import DocumentConverter
from dotenv import load_dotenv
from PIL import Image

load_dotenv()

IMAGE_FOLDER = Path("./images")
if not IMAGE_FOLDER.exists():
    IMAGE_FOLDER.mkdir()

DOCLING_HTTP_CLIENT = os.getenv("DOCLING_HTTP_CLIENT")
DOCLING_API_KEY = os.getenv("DOCLING_API_KEY")


class Converter:
    def __init__(self, lib: str, input_file: Path):
        self.lib = lib
        self.input_file = input_file
        fd, temp_path = tempfile.mkstemp(suffix=".md")
        self.output_file = Path(temp_path)  # Store as Path object for easy handling
        self.doc_image_folder = Path(f"{IMAGE_FOLDER}/{self.output_file.stem}")
        self.doc_image_folder.mkdir(parents=True, exist_ok=True)
        self.md_content = ""
        self.create_image_zip_file = False

    def has_image_extraction(self):
        return self.lib.lower() in ["mistral-ocr"]

    def extract_images_from_pdf(self):
        pdf_document = fitz.open(self.input_file)
        img_index = 0
        # Iterate through the pages
        for page_num in range(len(pdf_document)):
            page = pdf_document.load_page(page_num)
            images = page.get_images(full=True)
            # Iterate through the images on the page
            for i, img in enumerate(images):
                try:
                    xref = img[0]
                    base_image = pdf_document.extract_image(xref)
                    image_bytes = base_image["image"]
                    image = Image.open(io.BytesIO(image_bytes))
                    # Save the image
                    image_path = self.doc_image_folder / f"img_{img_index}.png"
                    logging(image_path.name)
                    image.save(image_path)
                except Exception as e:
                    logging(f"Error extracting image: {str(e)}")
                img_index += 1

    def pymupdf_conversion(self):
        """Convert PDF to markdown using PyMuPDF (fitz) for text extraction and custom formatting"""
        doc = fitz.open(self.input_file)
        text_blocks = []
        for page_num in range(len(doc)):
            page = doc[page_num]
            # Get text blocks with formatting information
            blocks = page.get_text("dict")["blocks"]

            for block in blocks:
                if "lines" in block:
                    for line in block["lines"]:
                        if "spans" in line:
                            line_text = ""
                            is_bold = False
                            is_heading = False
                            font_size = 0

                            for span in line["spans"]:
                                # Check for formatting hints
                                if span["text"].strip():
                                    current_font_size = span["size"]
                                    current_font = span["font"].lower()
                                    current_text = span["text"]

                                    # Detect possible headings based on font size
                                    if current_font_size > font_size:
                                        font_size = current_font_size

                                    # Detect bold text
                                    if "bold" in current_font or span["flags"] & 2:  # 2 is bold flag
                                        is_bold = True

                                    line_text += current_text

                            if line_text.strip():
                                # Determine if this might be a heading based on font size
                                if font_size > 12:  # Arbitrary threshold - adjust as needed
                                    is_heading = True

                                text_blocks.append(
                                    {
                                        "text": line_text.strip(),
                                        "is_bold": is_bold,
                                        "is_heading": is_heading,
                                        "font_size": font_size,
                                        "page": page_num + 1,
                                    }
                                )

        # Convert to markdown
        md_lines = []
        prev_block = None

        for block in text_blocks:
            text = block["text"].strip()
            # Skip empty lines
            if not text:
                continue

            # Detect headings based on formatting and content
            if block["is_heading"] or (len(text) < 80 and not text.endswith((".", ",", ";", ":", "?", "!"))):
                # Determine heading level based on font size
                if block["font_size"] >= 18:
                    md_lines.append(f"# {text}")
                elif block["font_size"] >= 16:
                    md_lines.append(f"## {text}")
                elif block["font_size"] >= 14:
                    md_lines.append(f"### {text}")
                elif block["is_bold"]:
                    md_lines.append(f"**{text}**")
                else:
                    md_lines.append(text)
            else:
                # Regular text paragraph
                if block["is_bold"]:
                    md_lines.append(f"**{text}**")
                else:
                    md_lines.append(text)

            # Add separator between blocks from different pages
            if prev_block and prev_block["page"] != block["page"]:
                md_lines.append("\n---\n")
            prev_block = block

        # Join all lines
        md_content = "\n\n".join(md_lines)
        md_content = re.sub(r"\n{3,}", "\n\n", md_content)
        return md_content

    def docling_serve_conversion(
        self,
        *,
        to_formats=("md",),  # Markdown
        image_export_mode="embedded",  # Embedded images
        pipeline="standard",  # Standard
        do_ocr=True,  # Enable OCR
        force_ocr=False,  # Force OCR off
        ocr_engine="easyocr",  # EasyOCR
        ocr_lang=("en", "fr", "de", "it"),  # beware-of-format -> we send JSON array
        pdf_backend="pypdfium2",  # PDF backend
        table_mode="accurate",  # Accurate
        abort_on_error=False,  # Abort on error (UI unchecked -> False)
        return_as_file=False,  # “Return as File” toggle -> ZIP
        include_images=True,
        images_scale=2,
        md_page_break_placeholder="",
        page_range=None,  # e.g., (1, 10)
        document_timeout=3600,  # seconds
        request_timeout=120,  # seconds
    ):
        base_url = DOCLING_HTTP_CLIENT
        if not base_url:
            raise RuntimeError("DOCLING_HTTP_CLIENT is not set.")
        url = f"{base_url.rstrip('/')}/v1/convert/file"

        # target type: inline JSON vs ZIP file
        target_type = "zip" if return_as_file else "inbody"

        headers = {"Authorization": f"Bearer {DOCLING_API_KEY}"}
        if not DOCLING_API_KEY:
            raise RuntimeError("DOCLING_API_KEY is not set.")

        data = {
            "to_formats": json.dumps(list(to_formats)),
            "target_type": target_type,
            "document_timeout": str(int(document_timeout)),
            "include_images": str(bool(include_images)).lower(),
            "image_export_mode": image_export_mode,  # embedded | placeholder | referenced
            "images_scale": str(images_scale),
            "md_page_break_placeholder": md_page_break_placeholder,
            "pipeline": pipeline,
            "do_ocr": str(bool(do_ocr)).lower(),
            "force_ocr": str(bool(force_ocr)).lower(),
            "ocr_engine": ocr_engine,  # easyocr | tesseract | rapidocr
            "ocr_lang": json.dumps(list(ocr_lang)),  # send JSON array to be safe
            "pdf_backend": pdf_backend,  # pypdfium2 | dlparse_v1/v2/v4
            "table_mode": table_mode,  # fast | accurate
            "abort_on_error": str(bool(abort_on_error)).lower(),
        }
        if page_range:
            data["page_range"] = json.dumps([int(page_range[0]), int(page_range[1])])

        try:
            with open(self.input_file, "rb") as f:
                files = {"files": (os.path.basename(self.input_file), f, "application/pdf")}

                with httpx.Client(timeout=request_timeout) as client:
                    response = client.post(
                        url,
                        headers=headers,
                        files=files,
                        data=data,
                    )

                if response.status_code != 200:
                    logging.error(
                        f"Failed to convert document {self.input_file}: {response.status_code} - {response.text}"
                    )
                    return ""

                result: dict[str, str | dict[str, str]] = response.json()
                if result.get("status") == "success" and "document" in result:
                    document: str | dict[str, str] = result["document"]
                    if isinstance(document, dict):
                        return document.get("md_content", "")
                    else:
                        logging.error(f"Failed to convert document {self.input_file}: {document}")
                        return ""
                else:
                    logging.error(
                        f"Failed to convert document {self.input_file}: {result.get('status')}. \n Errors: {result.get('errors')}"
                    )
                    return ""

        except Exception:
            logging.exception(f"Error converting document {self.input_file} via API.")
            return ""

    def pymupdf4llm_conversion(self):
        """Convert PDF to markdown using pymupdf4llm"""
        try:
            md_content = pymupdf4llm.to_markdown(self.input_file)
            return md_content
        except Exception as e:
            logging(f"pymupdf4llm conversion error: {str(e)}")
            return f"Conversion with pymupdf4llm failed: {str(e)}"

    def docling_conversion(self):
        """Convert PDF to markdown using docling"""
        try:
            doc = DocumentConverter()
            conversion_result = doc.convert(self.input_file)
            md_content = conversion_result.document.export_to_markdown()
            return md_content

        except Exception as e:
            logging(f"docling conversion error: {str(e)}")
            return f"Conversion with docling failed: {str(e)}"

    def pdfplumber_conversion(self):
        """Extracts text with headings and tables from a PDF while maintaining structure."""
        structured_text = []

        with pdfplumber.open(self.input_file) as pdf:
            for page_num, page in enumerate(pdf.pages, start=1):
                text_blocks = page.extract_words()  # Extract text blocks
                char_data = page.objects.get("char", [])  # Get character-level metadata

                font_sizes = [char["size"] for char in char_data if "size" in char]  # Extract font sizes
                avg_font_size = sum(font_sizes) / len(font_sizes) if font_sizes else 12  # Default to 12 if unknown

                # Process each word and infer headings based on font size
                for word in text_blocks:
                    text = word["text"]

                    # Find corresponding font size (fallback to avg)
                    word_font_size = next(
                        (char["size"] for char in char_data if char["text"] == text),
                        avg_font_size,
                    )

                    # Heading detection: If font size is significantly larger than the average, assume heading
                    if word_font_size > avg_font_size * 1.2:  # 20% larger than avg
                        structured_text.append(f"\n# {text}\n")  # Markdown heading
                    else:
                        structured_text.append(text)

                # Extract Tables
                tables = page.extract_tables()
                for table in tables:
                    structured_text.append("\n| " + " | ".join(table[0]) + " |\n")  # Markdown Table Header
                    structured_text.append("|" + " --- |" * len(table[0]))  # Table divider
                    for row in table[1:]:
                        structured_text.append("| " + " | ".join(row) + " |")

                structured_text.append("\n---\n")  # Page separator
        return "\n".join(structured_text)

    def zip_markdown_doc_with_images(self):
        with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tmp_zip_file:
            temp_zip_path = Path(tmp_zip_file.name)  # Get the temp file path

        with zipfile.ZipFile(temp_zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            # Add the output file to the root of the zip archive
            if self.output_file.exists():
                zipf.write(self.output_file, self.output_file.name)

            # Add image files inside "images/" directory in the zip archive
            if self.doc_image_folder.exists():
                for root, _, files in os.walk(self.doc_image_folder):
                    for file in files:
                        file_path = Path(root) / file
                        zipf.write(file_path, Path("images") / file)
        return Path(temp_zip_path)

    def get_zipped_images(self):
        shutil.make_archive(self.doc_image_folder, "zip", self.doc_image_folder)
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

    def convert(self):
        lib = self.lib.lower()
        if lib == "docling":
            self.md_content = self.docling_conversion()
        elif lib == "docling-serve":
            # Your tuned defaults from the UI
            self.md_content = self.docling_serve_conversion(
                to_formats=("md",),
                image_export_mode="embedded",
                pipeline="standard",
                do_ocr=True,
                force_ocr=False,
                ocr_engine="easyocr",
                ocr_lang=("en", "fr", "de", "it"),
                pdf_backend="pypdfium2",
                table_mode="accurate",
                abort_on_error=False,
                return_as_file=False,
            )
        elif lib == "pymupdf4llm":
            self.md_content = self.pymupdf4llm_conversion()
        else:
            self.md_content = self.pymupdf_conversion()

        with open(self.output_file, "w", encoding="utf-8") as f:
            f.write(self.md_content)
