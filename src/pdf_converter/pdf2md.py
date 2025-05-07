import base64
import io
import os
import re
import shutil
import tempfile
import zipfile
from pathlib import Path

import fitz  # PyMuPDF
import pdfplumber
import pymupdf4llm
from docling.document_converter import DocumentConverter
from PIL import Image

IMAGE_FOLDER = Path("./images")
if not IMAGE_FOLDER.exists():
    IMAGE_FOLDER.mkdir()


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
                    print(image_path.name)
                    image.save(image_path)
                except Exception as e:
                    print(f"Error extracting image: {str(e)}")
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
                                    if (
                                        "bold" in current_font or span["flags"] & 2
                                    ):  # 2 is bold flag
                                        is_bold = True

                                    line_text += current_text

                            if line_text.strip():
                                # Determine if this might be a heading based on font size
                                if (
                                    font_size > 12
                                ):  # Arbitrary threshold - adjust as needed
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
            if block["is_heading"] or (
                len(text) < 80 and not text.endswith((".", ",", ";", ":", "?", "!"))
            ):
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

    def pymupdf4llm_conversion(self):
        """Convert PDF to markdown using pymupdf4llm"""
        try:
            md_content = pymupdf4llm.to_markdown(self.input_file)
            return md_content
        except Exception as e:
            print(f"pymupdf4llm conversion error: {str(e)}")
            return f"Conversion with pymupdf4llm failed: {str(e)}"

    def docling_conversion(self):
        """Convert PDF to markdown using docling"""
        try:
            doc = DocumentConverter()
            conversion_result = doc.convert(self.input_file)
            md_content = conversion_result.document.export_to_markdown()
            return md_content

        except Exception as e:
            print(f"docling conversion error: {str(e)}")
            return f"Conversion with docling failed: {str(e)}"

    def pdfplumber_conversion(self):
        """Extracts text with headings and tables from a PDF while maintaining structure."""
        structured_text = []

        with pdfplumber.open(self.input_file) as pdf:
            for page_num, page in enumerate(pdf.pages, start=1):
                text_blocks = page.extract_words()  # Extract text blocks
                char_data = page.objects.get("char", [])  # Get character-level metadata

                font_sizes = [
                    char["size"] for char in char_data if "size" in char
                ]  # Extract font sizes
                avg_font_size = (
                    sum(font_sizes) / len(font_sizes) if font_sizes else 12
                )  # Default to 12 if unknown

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
                    structured_text.append(
                        "\n| " + " | ".join(table[0]) + " |\n"
                    )  # Markdown Table Header
                    structured_text.append(
                        "|" + " --- |" * len(table[0])
                    )  # Table divider
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
            mime_type = (
                "application/pdf"
                if self.output_file.suffix == ".pdf"
                else "text/markdown"
            )
            filename = os.path.basename(self.output_file)
            href = f'<a href="data:file/{mime_type};base64,{b64}" download="{filename}">{link_text}</a>'
            return href
        return None

    def convert(self):
        if self.lib.lower() == "docling":
            self.md_content = self.docling_conversion()
        elif self.lib.lower() == "pymupdf4llm":
            self.md_content = self.pymupdf4llm_conversion()
        else:
            self.md_content = self.pymupdf_conversion()
        with open(self.output_file, "w") as f:
            f.write(self.md_content)
