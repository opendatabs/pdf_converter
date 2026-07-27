import base64
import io
import logging
import os
import re
import shutil
import tempfile
import time
import zipfile
from pathlib import Path
from typing import Any

import fitz  # PyMuPDF
import httpx
import pdfplumber
import pymupdf4llm
from docling.document_converter import DocumentConverter
from dotenv import load_dotenv
from PIL import Image

load_dotenv()

logger = logging.getLogger(__name__)

IMAGE_FOLDER = Path("./images")
if not IMAGE_FOLDER.exists():
    IMAGE_FOLDER.mkdir()

DOCLING_HTTP_CLIENT = os.getenv("DOCLING_HTTP_CLIENT")
DOCLING_API_KEY = os.getenv("DOCLING_API_KEY")

_TERMINAL_TASK_STATUSES = frozenset({"success", "failure", "partial_success", "skipped"})
_SUCCESS_TASK_STATUSES = frozenset({"success", "partial_success"})
_DEFAULT_POLL_WAIT_SECONDS = 30.0
_POLL_TIMEOUT_BUFFER_SECONDS = 15.0
_DEFAULT_SUBMIT_TIMEOUT_SECONDS = 120.0
_DEFAULT_RESULT_TIMEOUT_SECONDS = 120.0


def _format_http_error(exc: BaseException) -> str:
    """Build a readable error string, including HTTP response body when available."""
    if isinstance(exc, httpx.TimeoutException):
        cause = f"{type(exc).__name__}: {exc}"
        if exc.__cause__ is not None:
            cause = f"{cause} (cause={exc.__cause__!r})"
        return cause
    if isinstance(exc, httpx.HTTPStatusError):
        body = (exc.response.text or "").strip().replace("\n", " ")
        if len(body) > 500:
            body = f"{body[:500]}..."
        return f"{exc} | response={body}" if body else str(exc)
    return f"{type(exc).__name__}: {exc}"


def _submit_docling_async_task(
    client: httpx.Client,
    *,
    base_url: str,
    headers: dict[str, str],
    input_file: Path,
    form_data: dict[str, Any],
    timeout: float,
) -> str:
    """Submit a file via ``/v1/convert/file/async`` and return the task id.

    Args:
        client: Shared httpx client.
        base_url: Docling Serve base URL.
        headers: Request headers including authorization.
        input_file: Path to the PDF to convert.
        form_data: Multipart form fields for conversion options.
        timeout: HTTP timeout for the submit request in seconds.

    Returns:
        The task id assigned by Docling Serve.

    Raises:
        TimeoutError: If the submit request times out.
        RuntimeError: If the submit request fails or response lacks a task_id.
    """
    url = f"{base_url.rstrip('/')}/v1/convert/file/async"
    timeout_config = httpx.Timeout(connect=30.0, read=timeout, write=timeout, pool=30.0)
    logger.info("Submitting Docling async job to %s (timeout=%.0fs)", url, timeout)
    started = time.monotonic()
    try:
        with input_file.open("rb") as file_handle:
            files = {"files": (input_file.name, file_handle, "application/pdf")}
            response = client.post(
                url,
                headers=headers,
                files=files,
                data=form_data,
                timeout=timeout_config,
            )
    except httpx.TimeoutException as exc:
        elapsed = time.monotonic() - started
        raise TimeoutError(
            f"submit {type(exc).__name__} after {elapsed:.1f}s "
            f"(configured {timeout:.0f}s). Async submit must return a task_id within "
            f"seconds — Docling Serve may be blocked or a proxy is killing the request. "
            f"Detail: {exc}"
        ) from exc
    except httpx.HTTPError as exc:
        elapsed = time.monotonic() - started
        raise RuntimeError(
            f"submit failed after {elapsed:.1f}s to {url}: {_format_http_error(exc)}"
        ) from exc

    if response.status_code >= 400:
        body = (response.text or "").strip().replace("\n", " ")[:500]
        raise RuntimeError(f"submit HTTP {response.status_code} from {url}: {body}")

    payload = response.json()
    task_id = payload.get("task_id")
    if not task_id:
        raise RuntimeError(f"Async submit response missing task_id: {payload}")
    return str(task_id)


def _poll_docling_task_until_done(
    client: httpx.Client,
    *,
    base_url: str,
    headers: dict[str, str],
    task_id: str,
    poll_wait: float,
    deadline: float,
) -> dict[str, Any]:
    """Long-poll until the task reaches a terminal status.

    Individual poll HTTP timeouts are retried until ``deadline``. Only the overall
    deadline fails the conversion — a single slow poll must not abort a 1h job.

    Args:
        client: Shared httpx client.
        base_url: Docling Serve base URL.
        headers: Request headers including authorization.
        task_id: Async task id from submit.
        poll_wait: Seconds to wait per poll request.
        deadline: Monotonic clock deadline for the overall poll loop.

    Returns:
        The final TaskStatusResponse payload.

    Raises:
        TimeoutError: If the overall deadline is exceeded before completion.
        httpx.HTTPStatusError: If a poll request fails with a non-timeout HTTP error.
    """
    url = f"{base_url.rstrip('/')}/v1/status/poll/{task_id}"
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(
                f"poll deadline exceeded for task {task_id} "
                f"(document_timeout budget exhausted)"
            )

        wait = min(poll_wait, remaining)
        request_timeout = wait + _POLL_TIMEOUT_BUFFER_SECONDS
        try:
            response = client.get(
                url,
                headers=headers,
                params={"wait": wait},
                timeout=request_timeout,
            )
            response.raise_for_status()
        except httpx.TimeoutException:
            logger.warning(
                "Docling poll timed out for task %s after %.0fs; retrying (%.0fs left)",
                task_id,
                request_timeout,
                deadline - time.monotonic(),
            )
            continue

        status_payload: dict[str, Any] = response.json()
        task_status = str(status_payload.get("task_status", ""))
        logger.info(
            "Docling task %s status=%s (%.0fs left)",
            task_id,
            task_status,
            deadline - time.monotonic(),
        )
        if task_status in _TERMINAL_TASK_STATUSES:
            return status_payload

    raise TimeoutError(
        f"poll deadline exceeded for task {task_id} (document_timeout budget exhausted)"
    )


def _fetch_docling_task_result(
    client: httpx.Client,
    *,
    base_url: str,
    headers: dict[str, str],
    task_id: str,
    timeout: float,
) -> str:
    """Fetch markdown content for a completed conversion task.

    Args:
        client: Shared httpx client.
        base_url: Docling Serve base URL.
        headers: Request headers including authorization.
        task_id: Completed async task id.
        timeout: HTTP timeout for the result request in seconds.

    Returns:
        Markdown content on success.

    Raises:
        httpx.HTTPStatusError: If the result request fails.
        TimeoutError: If the result request times out.
        RuntimeError: If the result payload is missing or invalid.
    """
    url = f"{base_url.rstrip('/')}/v1/result/{task_id}"
    timeout_config = httpx.Timeout(connect=30.0, read=timeout, write=timeout, pool=30.0)
    try:
        response = client.get(url, headers=headers, timeout=timeout_config)
    except httpx.TimeoutException as exc:
        raise TimeoutError(
            f"result fetch {type(exc).__name__} after configured {timeout:.0f}s for task {task_id}: {exc}"
        ) from exc
    response.raise_for_status()
    result: dict[str, Any] = response.json()
    status = result.get("status")
    if status not in _SUCCESS_TASK_STATUSES or "document" not in result:
        raise RuntimeError(
            f"Docling task {task_id} result unsuccessful: status={status}, errors={result.get('errors')}"
        )

    document = result["document"]
    if not isinstance(document, dict):
        raise RuntimeError(f"Docling task {task_id} result document is not a dict: {document!r}")

    md_content = document.get("md_content", "")
    if not isinstance(md_content, str):
        raise RuntimeError(f"Docling task {task_id} md_content is not a string: {type(md_content)!r}")
    return md_content


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
        self.last_error: str | None = None

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
        to_formats: list[str] | None = None,
        image_export_mode: str = "embedded",
        pipeline: str = "standard",
        do_ocr: bool = True,
        force_ocr: bool = False,
        ocr_engine: str = "easyocr",
        ocr_lang: list[str] | None = None,
        pdf_backend: str = "pypdfium2",
        table_mode: str = "accurate",
        abort_on_error: bool = False,
        return_as_file: bool = False,
        include_images: bool = True,
        images_scale: float = 2,
        md_page_break_placeholder: str = "",
        page_range: list[int] | None = None,
        document_timeout: float = 3600,
        request_timeout: float = _DEFAULT_RESULT_TIMEOUT_SECONDS,
        submit_timeout: float = _DEFAULT_SUBMIT_TIMEOUT_SECONDS,
        poll_wait: float = _DEFAULT_POLL_WAIT_SECONDS,
    ) -> str | None:
        """Convert a PDF via Docling Serve async submit/poll/result flow.

        Submits the file to ``/v1/convert/file/async``, long-polls
        ``/v1/status/poll/{task_id}`` until the task finishes, then fetches
        markdown from ``/v1/result/{task_id}``. This avoids holding a single
        HTTP connection open for the full conversion duration.

        Args:
            to_formats: Output formats requested from Docling Serve.
            image_export_mode: Image export mode (embedded, placeholder, referenced).
            pipeline: Processing pipeline name.
            do_ocr: Whether to run OCR on bitmap content.
            force_ocr: Whether to replace existing text with OCR text.
            ocr_engine: OCR engine name (deprecated server-side; still accepted).
            ocr_lang: Languages passed to the OCR engine.
            pdf_backend: PDF backend used by Docling Serve.
            table_mode: Table structure mode (fast or accurate).
            abort_on_error: Whether conversion should abort on first error.
            return_as_file: If True, request zip target; otherwise inbody JSON.
            include_images: Whether to include extracted images.
            images_scale: Scale factor for extracted images.
            md_page_break_placeholder: Placeholder inserted between markdown pages.
            page_range: Optional inclusive page range starting at 1.
            document_timeout: Server-side per-document timeout in seconds; also
                used as the client-side overall poll deadline.
            request_timeout: Timeout in seconds for the final result fetch.
            submit_timeout: Timeout in seconds for async submit/upload. A healthy
                async endpoint should return a task_id within seconds.
            poll_wait: Seconds to wait on each status poll request.

        Returns:
            Markdown content on success, or None on failure.

        Raises:
            RuntimeError: If required Docling Serve env vars are missing.
        """
        base_url = DOCLING_HTTP_CLIENT
        if not base_url:
            raise RuntimeError("DOCLING_HTTP_CLIENT is not set.")
        if not DOCLING_API_KEY:
            raise RuntimeError("DOCLING_API_KEY is not set.")

        if to_formats is None:
            to_formats = ["md"]
        if ocr_lang is None:
            ocr_lang = ["en", "fr", "de", "it"]

        target_type = "zip" if return_as_file else "inbody"
        headers = {"Authorization": f"Bearer {DOCLING_API_KEY}"}
        form_data: dict[str, Any] = {
            "to_formats": to_formats,
            "target_type": target_type,
            "document_timeout": document_timeout,
            "include_images": include_images,
            "image_export_mode": image_export_mode,
            "images_scale": images_scale,
            "md_page_break_placeholder": md_page_break_placeholder,
            "pipeline": pipeline,
            "do_ocr": do_ocr,
            "force_ocr": force_ocr,
            "ocr_engine": ocr_engine,
            "ocr_lang": ocr_lang,
            "pdf_backend": pdf_backend,
            "table_mode": table_mode,
            "abort_on_error": abort_on_error,
        }
        if page_range:
            form_data["page_range"] = page_range

        deadline = time.monotonic() + float(document_timeout)
        self.last_error = None
        try:
            # No client-level read timeout — each phase sets an explicit Timeout.
            with httpx.Client(verify=False, timeout=None) as client:
                task_id = _submit_docling_async_task(
                    client,
                    base_url=base_url,
                    headers=headers,
                    input_file=self.input_file,
                    form_data=form_data,
                    timeout=submit_timeout,
                )
                logger.info(
                    "Submitted Docling async task %s (poll up to %.0fs)",
                    task_id,
                    document_timeout,
                )

                status_payload = _poll_docling_task_until_done(
                    client,
                    base_url=base_url,
                    headers=headers,
                    task_id=task_id,
                    poll_wait=poll_wait,
                    deadline=deadline,
                )
                task_status = str(status_payload.get("task_status", ""))
                if task_status not in _SUCCESS_TASK_STATUSES:
                    error_message = status_payload.get("error_message")
                    self.last_error = (
                        f"Docling async task {task_id} failed: "
                        f"status={task_status}, error_message={error_message}"
                    )
                    logger.error(self.last_error)
                    return None

                return _fetch_docling_task_result(
                    client,
                    base_url=base_url,
                    headers=headers,
                    task_id=task_id,
                    timeout=request_timeout,
                )
        except (httpx.HTTPError, TimeoutError, RuntimeError) as exc:
            self.last_error = (
                f"Docling Serve async conversion failed for {self.input_file}: {_format_http_error(exc)}"
            )
            logger.error(self.last_error)
            return None

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
                to_formats=["md"],
                image_export_mode="embedded",
                pipeline="standard",
                do_ocr=True,
                force_ocr=False,
                ocr_engine="easyocr",
                ocr_lang=["en", "fr", "de", "it"],
                pdf_backend="pypdfium2",
                table_mode="accurate",
                abort_on_error=False,
                return_as_file=False,
            )
        elif lib == "pymupdf4llm":
            self.md_content = self.pymupdf4llm_conversion()
        else:
            self.md_content = self.pymupdf_conversion()

        if self.md_content is None:
            # Keep subprocess output contract stable: failed conversions emit empty output.
            if not self.last_error:
                self.last_error = f"Conversion with {lib} returned no content"
            self.md_content = ""

        with open(self.output_file, "w", encoding="utf-8") as f:
            f.write(self.md_content)
