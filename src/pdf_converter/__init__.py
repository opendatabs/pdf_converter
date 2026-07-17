import json
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


def _failures_path(zip_path: Path) -> Path:
    """Path to the failure-count JSON stored next to the output zip."""
    return Path(zip_path).with_name(f"{Path(zip_path).stem}.failures.json")


def _load_failures(zip_path: Path) -> dict[str, int]:
    path = _failures_path(zip_path)
    if not path.exists():
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            logging.warning(f"Ignoring invalid failure store {path}: expected a JSON object")
            return {}
        return {str(k): int(v) for k, v in data.items()}
    except (OSError, json.JSONDecodeError, TypeError, ValueError) as e:
        logging.warning(f"Could not load failure counts from {path}: {e}")
        return {}


def _save_failures(zip_path: Path, failures: dict[str, int]) -> None:
    path = _failures_path(zip_path)
    if not failures:
        if path.exists():
            path.unlink()
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(failures, f, indent=2, sort_keys=True)


def _record_failure(failures: dict[str, int], zip_path: Path, filename: str, max_failures: int) -> None:
    count = failures.get(filename, 0) + 1
    failures[filename] = count
    logging.warning(f"Conversion failed for {filename}: {count}/{max_failures}")
    _save_failures(zip_path, failures)


def _clear_failure(failures: dict[str, int], zip_path: Path, filename: str) -> None:
    if filename not in failures:
        return
    del failures[filename]
    _save_failures(zip_path, failures)


def _prepare_batch_rows(
    df: pd.DataFrame,
    name_column: str,
    url_column: str,
    suffix: str,
    existing: set[str],
    replace_all: bool,
    failures: dict[str, int],
    max_failures: int | None,
    shuffle: bool,
) -> pd.DataFrame:
    valid = df[[name_column, url_column]].dropna()
    valid = valid[valid[url_column].astype(str).str.len() > 0]

    filenames = _build_filenames(valid[name_column], suffix)
    valid = valid.assign(__filename=filenames)

    if not replace_all:
        valid = valid[~valid["__filename"].isin(existing)]

    valid = valid.drop_duplicates(subset="__filename", keep="first")

    if max_failures is not None:
        # Drop stale failure entries for files already present in the zip
        for name in [n for n in failures if n in existing]:
            del failures[name]

        exhausted = {name for name, count in failures.items() if count >= max_failures}
        if exhausted:
            skipped = valid[valid["__filename"].isin(exhausted)]
            for filename in skipped["__filename"]:
                count = failures[filename]
                logging.info(f"Skipping {filename}: reached max failures ({count}/{max_failures})")
            valid = valid[~valid["__filename"].isin(exhausted)]

    if shuffle and not valid.empty:
        valid = valid.sample(frac=1).reset_index(drop=True)

    return valid


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
    max_failures: int | None = None,
    shuffle: bool = False,
):
    """
    Convert PDFs referenced in a DataFrame column to Markdown and store them in a zip.

    Files already present in the zip are skipped unless ``replace_all`` is True.
    Only successful (non-blank) conversions are written.

    Args:
        df: Source DataFrame.
        url_column: Column with PDF URLs.
        method: Conversion backend name (passed to ``convert_pdf_to_md``).
        zip_path: Output zip path. Failure counts are stored beside it as
            ``{zip_stem}.failures.json`` when ``max_failures`` is set.
        md_name_column: Column used to build output filenames.
        replace_all: If True, re-convert even when the target already exists in the zip.
        max_failures: If set, skip a filename after it has failed this many times across
            runs (empty/blank output, download or subprocess failure). ``None`` retries
            forever (default). A later successful write clears the failure entry.
        shuffle: If True, shuffle remaining work after filtering existing files and
            exhausted failures. Helps with transient / rate-limit failures by trying
            documents in a different order each run.
    """
    zip_path = Path(zip_path)
    existing = _ensure_zip(zip_path)
    suffix = f"_{method}.md"
    failures = _load_failures(zip_path) if max_failures is not None else {}

    valid = _prepare_batch_rows(
        df,
        md_name_column,
        url_column,
        suffix,
        existing,
        replace_all,
        failures,
        max_failures,
        shuffle,
    )
    if max_failures is not None:
        _save_failures(zip_path, failures)

    if valid.empty:
        logging.info("Nothing to do: all Markdown files already present.")
        return

    progress_bar = tqdm(total=len(valid), desc=f"Markdown ({method})", dynamic_ncols=True)

    for row in valid[[url_column, "__filename"]].itertuples(index=False, name=None):
        url, filename = row
        markdown = convert_pdf_to_md(url, method)
        wrote = False
        if markdown.strip():
            try:
                replace_in_zip(zip_path, filename, markdown)
                existing.add(filename)
                wrote = True
                if max_failures is not None:
                    _clear_failure(failures, zip_path, filename)
            except Exception as e:
                logging.error(f"⚠️ Failed to write {filename} to ZIP: {e}")

        if not wrote and max_failures is not None:
            _record_failure(failures, zip_path, filename, max_failures)

        progress_bar.update(1)
        if wrote:
            tqdm.write(f"Markdown created: {filename}")

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
    max_failures: int | None = None,
    shuffle: bool = False,
):
    """
    Convert PDFs referenced in a DataFrame column to plain text and store them in a zip.

    Files already present in the zip are skipped unless ``replace_all`` is True.
    Only successful (non-blank) conversions are written.

    Args:
        df: Source DataFrame.
        url_column: Column with PDF URLs.
        method: Conversion backend name (passed to ``convert_pdf_to_txt``).
        zip_path: Output zip path. Failure counts are stored beside it as
            ``{zip_stem}.failures.json`` when ``max_failures`` is set.
        txt_name_column: Column used to build output filenames.
        replace_all: If True, re-convert even when the target already exists in the zip.
        max_failures: If set, skip a filename after it has failed this many times across
            runs (empty/blank output, download or subprocess failure). ``None`` retries
            forever (default). A later successful write clears the failure entry.
        shuffle: If True, shuffle remaining work after filtering existing files and
            exhausted failures. Helps with transient / rate-limit failures by trying
            documents in a different order each run.
    """
    zip_path = Path(zip_path)
    existing = _ensure_zip(zip_path)
    suffix = f"_{method}.txt"
    failures = _load_failures(zip_path) if max_failures is not None else {}

    valid = _prepare_batch_rows(
        df,
        txt_name_column,
        url_column,
        suffix,
        existing,
        replace_all,
        failures,
        max_failures,
        shuffle,
    )
    if max_failures is not None:
        _save_failures(zip_path, failures)

    if valid.empty:
        logging.info("Nothing to do: all text files already present.")
        return

    progress_bar = tqdm(total=len(valid), desc=f"Text ({method})", dynamic_ncols=True)

    for row in valid[[url_column, "__filename"]].itertuples(index=False, name=None):
        url, filename = row
        text = convert_pdf_to_txt(url, method)
        wrote = False
        if text.strip():
            try:
                replace_in_zip(zip_path, filename, text)
                existing.add(filename)
                wrote = True
                if max_failures is not None:
                    _clear_failure(failures, zip_path, filename)
            except Exception as e:
                logging.error(f"⚠️ Failed to write {filename} to ZIP: {e}")

        if not wrote and max_failures is not None:
            _record_failure(failures, zip_path, filename, max_failures)

        progress_bar.update(1)
        if wrote:
            tqdm.write(f"Text created: {filename}")

    progress_bar.close()
    logging.info(f"Processed {len(valid)} rows for text conversion using '{method}'")

    logging.info(f"Unzipping {zip_path} to {zip_path.with_suffix('')}")
    unzip_to_folder(zip_path, zip_path.with_suffix(""), overwrite=True)
