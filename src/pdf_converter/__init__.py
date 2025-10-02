import logging
import re
import subprocess
import sys
import zipfile
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm

SCRIPT_DIR = Path(__file__).resolve().parent
CONVERT_SCRIPT_MD = SCRIPT_DIR / "convert_single_pdf2md.py"
CONVERT_SCRIPT_TXT = SCRIPT_DIR / "convert_single_pdf2txt.py"


def safe_filename(name):
    # Convert name to string and replace invalid characters
    if not isinstance(name, str):
        name = str(name)
    return re.sub(r"[^a-zA-Z0-9_\-.]", "_", name)


def replace_in_zip(zip_path: Path, filename: str, content: bytes):
    temp_zip_path = zip_path.with_suffix(".tmp.zip")
    with (
        zipfile.ZipFile(zip_path, "r") as zf_in,
        zipfile.ZipFile(temp_zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf_out,
    ):
        for item in zf_in.infolist():
            if item.filename != filename:
                zf_out.writestr(item, zf_in.read(item.filename))
        zf_out.writestr(filename, content)
    temp_zip_path.replace(zip_path)


def _ensure_zip(zip_path: Path) -> set[str]:
    """Make sure zip exists; return existing names."""
    if Path(zip_path).exists():
        with zipfile.ZipFile(zip_path, mode="r") as zf:
            return set(zf.namelist())
    logging.warning(f"ZIP {zip_path} does not exist. Creating a new one.")
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, mode="w"):
        pass
    return set()


def _build_filenames(series: pd.Series, suffix: str) -> pd.Series:
    # Vectorized safe filenames
    return series.astype(str).map(safe_filename) + suffix


def unzip_to_folder(zip_path: Path, target_dir: Path, overwrite: bool = False):
    """
    Extracts a ZIP to a normal folder.

    Args:
        zip_path (Path): Path to the ZIP file.
        target_dir (Path): Directory where contents will be extracted.
        overwrite (bool): If True, overwrite existing files.
    """
    target_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        for member in zf.namelist():
            target_file = target_dir / member
            if not overwrite and target_file.exists():
                continue
            zf.extract(member, target_dir)


def convert_pdf_to_md(pdf_url: str, method: str, pdf_path: Path = Path("temp.pdf")) -> str:
    """
    Downloads a PDF from a URL and converts it to Markdown using the specified conversion method.

    Args:
        pdf_url (str): The URL of the PDF to download.
        method (str): The conversion method to use (e.g. 'poppler', 'pdf2text').
        pdf_path (Path, optional): Path to save the downloaded PDF. Defaults to 'temp.pdf'.

    Returns:
        str: The Markdown content as a string, or an error message if the process fails.
    """
    logging.info(f"Downloading PDF: {pdf_url}")
    try:
        r_pdf = requests.get(pdf_url, timeout=10)
        r_pdf.raise_for_status()
        with open(pdf_path, "wb") as file:
            file.write(r_pdf.content)
    except Exception as e:
        logging.error(f"Failed to download PDF: {e}")
        return ""

    # Subprocess for crash isolation
    try:
        result = subprocess.run(
            [sys.executable, str(CONVERT_SCRIPT_MD), str(pdf_path), method],
            capture_output=True,
            text=True,
            timeout=300,
        )
        if result.returncode != 0:
            logging.error(f"[ERROR] Subprocess failed: {result.stderr}")
            return ""
        return result.stdout
    except subprocess.TimeoutExpired:
        logging.error("Conversion timed out.")
        return ""
    except Exception as e:
        logging.error(f"Unexpected error in subprocess: {e}")
        return ""


def create_markdown_from_column(
    df: pd.DataFrame,
    url_column: str,
    method: str,
    zip_path: Path,
    md_name_column: str,
    replace_all: bool = False,
):
    existing = _ensure_zip(zip_path)
    suffix = f"_{method}.md"

    # Base filters: valid name+url
    valid = df[[md_name_column, url_column]].dropna()
    valid = valid[valid[url_column].astype(str).str.len() > 0]

    # Precompute filenames
    filenames = _build_filenames(valid[md_name_column], suffix)
    valid = valid.assign(__filename=filenames)

    # If not replacing, drop rows whose target already exists
    if not replace_all:
        valid = valid[~valid["__filename"].isin(existing)]

    # De-dup by target filename to avoid redoing same file
    valid = valid.drop_duplicates(subset="__filename", keep="first")

    if valid.empty:
        logging.info("Nothing to do: all Markdown files already present.")
        return

    progress_bar = tqdm(total=len(valid), desc=f"Markdown ({method})", dynamic_ncols=True)

    # Iterate only filtered rows, use itertuples for speed
    for url, filename in valid[[url_column, "__filename"]].itertuples(index=False, name=None):
        markdown = convert_pdf_to_md(url, method)
    
        if not markdown or not markdown.strip():
            logging.warning(f"⚠️ No markdown produced for {filename} (url={url}). Skipping.")
            progress_bar.update(1)
            continue
    
        try:
            # be explicit about encoding
            replace_in_zip(Path(zip_path), filename, markdown.encode("utf-8"))
            existing.add(filename)
            tqdm.write(f"✅ wrote {filename} ({len(markdown)} chars)")
        except Exception as e:
            logging.error(f"⚠️ Failed to write {filename} to ZIP: {e}")
        finally:
            progress_bar.update(1)

    progress_bar.close()
    logging.info(f"Processed {len(valid)} rows for Markdown conversion using '{method}'")

    logging.info(f"Unzipping {zip_path} to {zip_path.with_suffix('')}")
    unzip_to_folder(zip_path, zip_path.with_suffix(""), overwrite=True)


def convert_pdf_to_txt(pdf_url: str, method: str, pdf_path: Path = Path("temp.pdf")) -> str:
    """
    Downloads a PDF from a URL and converts it to plain text using the specified method.

    Args:
        pdf_url (str): The URL of the PDF to download.
        method (str): The conversion method to use ('pymupdf', 'pdfplumber', etc.).
        pdf_path (Path, optional): Path to save the downloaded PDF. Defaults to 'temp.pdf'.

    Returns:
        str: The plain text content as a string, or an error message if the process fails.
    """
    logging.info(f"Downloading PDF: {pdf_url}")
    try:
        r_pdf = requests.get(pdf_url, timeout=10)
        r_pdf.raise_for_status()
        with open(pdf_path, "wb") as file:
            file.write(r_pdf.content)
    except Exception as e:
        logging.error(f"Failed to download PDF: {e}")
        return ""

    try:
        result = subprocess.run(
            [sys.executable, str(CONVERT_SCRIPT_TXT), str(pdf_path), method],
            capture_output=True,
            text=True,
            timeout=300,
        )
        if result.returncode != 0:
            logging.error(f"[ERROR] Subprocess failed: {result.stderr}")
            return ""
        return result.stdout
    except subprocess.TimeoutExpired:
        logging.error("Conversion timed out.")
        return ""
    except Exception as e:
        logging.error(f"Unexpected error in subprocess: {e}")
        return ""


def create_text_from_column(
    df: pd.DataFrame,
    url_column: str,
    method: str,
    zip_path: Path,
    txt_name_column: str,
    replace_all: bool = False,
):
    existing = _ensure_zip(zip_path)
    suffix = f"_{method}.txt"

    valid = df[[txt_name_column, url_column]].dropna()
    valid = valid[valid[url_column].astype(str).str.len() > 0]

    filenames = _build_filenames(valid[txt_name_column], suffix)
    valid = valid.assign(__filename=filenames)

    if not replace_all:
        valid = valid[~valid["__filename"].isin(existing)]

    valid = valid.drop_duplicates(subset="__filename", keep="first")

    if valid.empty:
        logging.info("Nothing to do: all text files already present.")
        return

    progress_bar = tqdm(total=len(valid), desc=f"Text ({method})", dynamic_ncols=True)

    for row in valid[[url_column, "__filename"]].itertuples(index=False, name=None):
        url, filename = row
        text = convert_pdf_to_txt(url, method)
        if text.strip():
            try:
                replace_in_zip(Path(zip_path), filename, text)
                existing.add(filename)
            except Exception as e:
                logging.error(f"⚠️ Failed to write {filename} to ZIP: {e}")
        progress_bar.update(1)
        tqdm.write(f"Text created: {filename}")

    progress_bar.close()
    logging.info(f"Processed {len(valid)} rows for text conversion using '{method}'")

    logging.info(f"Unzipping {zip_path} to {zip_path.with_suffix('')}")
    unzip_to_folder(zip_path, zip_path.with_suffix(""), overwrite=True)
