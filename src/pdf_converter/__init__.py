import logging
import re
import subprocess
import sys
import zipfile
from pathlib import Path

import pandas as pd
import requests

SCRIPT_DIR = Path(__file__).resolve().parent
CONVERT_SCRIPT_MD = SCRIPT_DIR / "convert_single_pdf2md.py"
CONVERT_SCRIPT_TXT = SCRIPT_DIR / "convert_single_pdf2txt.py"


def safe_filename(name):
    return re.sub(r"[^a-zA-Z0-9_\-.]", "_", name)


def replace_in_zip(zip_path, filename, content):
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
) -> pd.DataFrame:
    """
    Converts PDFs to Markdown and stores them in a ZIP archive.
    Skips any file that already exists in the ZIP.

    Args:
        df (pd.DataFrame): Input DataFrame.
        url_column (str): Column with PDF URLs.
        method (str): Conversion method to use in `convert_pdf_to_md`.
        zip_path (Path): Path to a ZIP file to cache/load Markdown files.
        md_name_column (str): Column used for naming Markdown files in the ZIP.
        replace_all (bool, optional): If True, replaces existing files in the ZIP. Defaults to False.
    Returns:
        pd.DataFrame: DataFrame with the Markdown column populated.
    """
    # Preload list of existing files in ZIP
    existing_zip_names = set()
    if Path(zip_path).exists():
        with zipfile.ZipFile(zip_path, mode="r") as zf:
            existing_zip_names = set(zf.namelist())
    else:
        print(f"⚠️ ZIP file {zip_path} does not exist. Creating a new one.")
        zip_path.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_path, mode="w") as zf:
            pass

    for idx, row in df.iterrows():
        filename = f"{safe_filename(row[md_name_column])}_{method}.md"

        # Generate and write Markdown if it doesn't exist in the ZIP
        if replace_all or filename not in existing_zip_names:
            markdown = convert_pdf_to_md(row[url_column], method)
            if markdown.strip():
                try:
                    replace_in_zip(Path(zip_path), filename, markdown)
                    existing_zip_names.add(filename)
                except Exception as e:
                    print(f"⚠️ Failed to write {filename} to ZIP: {e}")

    return df


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
            [sys.executable, str(SCRIPT_DIR / "convert_single_pdf_txt.py"), str(pdf_path), method],
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
) -> pd.DataFrame:
    """
    Converts PDFs to plain text and stores them in a ZIP archive.
    Skips any file that already exists in the ZIP.

    Args:
        df (pd.DataFrame): Input DataFrame.
        url_column (str): Column with PDF URLs.
        method (str): Conversion method to use in `convert_pdf_to_txt`.
        zip_path (Path): Path to a ZIP file to cache/load text files.
        txt_name_column (str): Column used for naming .txt files in the ZIP.
        replace_all (bool, optional): If True, replaces existing files in the ZIP. Defaults to False.

    Returns:
        pd.DataFrame: DataFrame with conversion status (if needed).
    """
    existing_zip_names = set()
    if Path(zip_path).exists():
        with zipfile.ZipFile(zip_path, mode="r") as zf:
            existing_zip_names = set(zf.namelist())
    else:
        print(f"⚠️ ZIP file {zip_path} does not exist. Creating a new one.")
        zip_path.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_path, mode="w") as zf:
            pass

    for idx, row in df.iterrows():
        filename = f"{safe_filename(row[txt_name_column])}_{method}.txt"

        if replace_all or filename not in existing_zip_names:
            text = convert_pdf_to_txt(row[url_column], method)
            if text.strip():
                try:
                    replace_in_zip(Path(zip_path), filename, text)
                    existing_zip_names.add(filename)
                except Exception as e:
                    print(f"⚠️ Failed to write {filename} to ZIP: {e}")

    return df
