"""Embedded-image extraction from PDFs.

Extra: ``images`` (installs ``pymupdf`` and ``pillow``). Not a conversion
backend — used alongside one when a document's pictures are wanted as files.
"""

import io
import logging
from pathlib import Path

from pdf_converter.backends.base import require

NAME = "images"
EXTRA = "images"

logger = logging.getLogger(__name__)


def extract_images(input_file: Path, output_folder: Path) -> list[Path]:
    """Write every embedded image of a PDF into ``output_folder`` as PNG.

    Args:
        input_file: PDF to read.
        output_folder: Destination folder; created if missing.

    Returns:
        Paths of the images that were written successfully.
    """
    fitz = require("fitz", backend=NAME, extra=EXTRA)
    pil_image = require("PIL.Image", backend=NAME, extra=EXTRA)

    output_folder.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []

    with fitz.open(input_file) as pdf_document:
        img_index = 0
        for page_num in range(len(pdf_document)):
            page = pdf_document.load_page(page_num)
            for img in page.get_images(full=True):
                try:
                    base_image = pdf_document.extract_image(img[0])
                    image = pil_image.open(io.BytesIO(base_image["image"]))
                    image_path = output_folder / f"img_{img_index}.png"
                    image.save(image_path)
                    logger.info("Extracted image %s", image_path.name)
                    written.append(image_path)
                except Exception as e:
                    logger.error("Error extracting image %d: %s", img_index, e)
                img_index += 1

    return written
