import logging
import subprocess
import sys
import zipfile
from io import BytesIO
from pathlib import Path

import pandas as pd
import requests

from pdf_converter.pdf2md import Converter

SCRIPT_DIR = Path(__file__).resolve().parent
CONVERT_SCRIPT = SCRIPT_DIR / "convert_single_pdf.py"


def replace_in_zip(zip_path, filename, content):
    temp_zip_path = zip_path.with_suffix(".tmp.zip")
    with zipfile.ZipFile(zip_path, "r") as zf_in, \
         zipfile.ZipFile(temp_zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf_out:
        for item in zf_in.infolist():
            if item.filename != filename:
                zf_out.writestr(item, zf_in.read(item.filename))
        zf_out.writestr(filename, content)
    temp_zip_path.replace(zip_path)
    

def convert_pdf_to_md(
    pdf_url: str, method: str, pdf_path: Path = Path("temp.pdf")
) -> str:
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
        print(f"Failed to download PDF: {e}")
        return ""

    # Subprocess for crash isolation
    try:
        result = subprocess.run(
            [sys.executable, str(CONVERT_SCRIPT), str(pdf_path), method],
            capture_output=True,
            text=True,
            timeout=300,
        )
        if result.returncode != 0:
            print(f"[ERROR] Subprocess failed: {result.stderr}")
            return ""
        return result.stdout
    except subprocess.TimeoutExpired:
        print("Conversion timed out.")
        return ""
    except Exception as e:
        print(f"Unexpected error in subprocess: {e}")
        return ""


def add_markdown_column(
    df: pd.DataFrame,
    url_column: str,
    method: str,
    md_column: str = None,
    csv_output_path: Path = None,
    zip_path: Path = None,
    md_name_column: str = None,
) -> pd.DataFrame:
    """
    Adds a column to the DataFrame containing Markdown converted from PDF URLs.
    If a Markdown file already exists in the ZIP (based on md_name_column), it is reused.
    Markdown files are written to the ZIP immediately after creation (ZIP opened per write).

    Args:
        df (pd.DataFrame): Input DataFrame.
        url_column (str): Column with PDF URLs.
        method (str): Conversion method to use in `convert_pdf_to_md`.
        md_column (str, optional): Name for the Markdown column.
            Defaults to '<url_column>_md_<method>'.
        csv_output_path (Path, optional): If provided, saves the DataFrame after each row.
        zip_path (Path, optional): Path to a ZIP file to cache/load Markdown files.
        md_name_column (str, optional): Column used for naming Markdown files in the ZIP.

    Returns:
        pd.DataFrame: DataFrame with the Markdown column populated.
    """
    if md_column is None:
        md_column = f"{url_column}_md_{method}"

    df = df.copy()
    if md_column not in df.columns:
        df[md_column] = None

    # Preload list of existing files in ZIP
    existing_zip_names = set()
    if zip_path and Path(zip_path).exists():
        with zipfile.ZipFile(zip_path, mode="r") as zf:
            existing_zip_names = set(zf.namelist())

    for idx, row in df.iterrows():
        if row[md_column] == "" or pd.isnull(row[md_column]):
            filename = f"{row[md_name_column]}_{method}.md" if md_name_column else None
            markdown = None

            # Reuse if already in ZIP
            if zip_path and filename and filename in existing_zip_names:
                try:
                    with zipfile.ZipFile(zip_path, mode="r") as zf:
                        with zf.open(filename) as f:
                            content = f.read().decode("utf-8").strip()
                            if content:
                                markdown = content
                            else:
                                print(f"⚠️ Cached file {filename} is empty. Will reprocess PDF.")
                                existing_zip_names.remove(filename)  # Force regeneration
                except Exception as e:
                    print(f"⚠️ Failed to read {filename} from ZIP: {e}")

            # Else generate and write
            if markdown is None or not markdown.strip():
                markdown = convert_pdf_to_md(row[url_column], method)
                if zip_path and filename and markdown.strip():
                    try:
                        replace_in_zip(Path(zip_path), filename, markdown)
                        existing_zip_names.add(filename)
                    except Exception as e:
                        print(f"⚠️ Failed to write {filename} to ZIP: {e}")

            df.at[idx, md_column] = markdown

            if csv_output_path:
                df.to_csv(csv_output_path, index=False)

    return df
