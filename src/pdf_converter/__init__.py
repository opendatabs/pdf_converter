import logging
import zipfile
from io import BytesIO
from pathlib import Path

import pandas as pd
import requests

from pdf_converter.pdf2md import Converter


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
    # Download the PDF
    logging.info(f"   Downloading PDF: {pdf_url}")
    try:
        r_pdf = requests.get(pdf_url)
        with open(pdf_path, "wb") as file:
            for chunk in r_pdf.iter_content(chunk_size=1024):
                file.write(chunk)
        print(f"PDF downloaded successfully as {pdf_path}")
    except requests.exceptions.RequestException as e:
        print(f"Failed to download PDF: {e}")
        return ""

    # Convert the PDF to Markdown
    converter = Converter(lib=method, input_file=pdf_path)
    try:
        converter.convert()
        markdown_path = converter.output_file
        with open(markdown_path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception as e:
        print(f"Failed to convert PDF to Markdown: {e}")
        return f""


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
    Adds a column to the DataFrame containing Markdown converted from PDF URLs,
    and optionally writes individual Markdown files into a ZIP archive using names
    derived from a specified DataFrame column.

    Args:
        df (pd.DataFrame): The input DataFrame.
        url_column (str): Column with PDF URLs to convert.
        method (str): Conversion method to use in `convert_pdf_to_md`.
        md_column (str, optional): Name for the new Markdown column.
            Defaults to '<url_column>_md_<method>'.
        csv_output_path (Path, optional): If provided, saves the full DataFrame after each row.
        zip_path (Path, optional): If provided, stores each Markdown file in this ZIP archive.
        md_name_column (str, optional): Column to use for naming Markdown files inside the ZIP.

    Returns:
        pd.DataFrame: A copy of the DataFrame with an additional column containing Markdown strings.
    """
    if md_column is None:
        md_column = f"{url_column}_md_{method}"

    df = df.copy()
    if md_column not in df.columns:
        df[md_column] = None

    zip_file = None
    if zip_path:
        zip_mode = "a" if zip_path.exists() else "w"
        zip_file = zipfile.ZipFile(zip_path, mode=zip_mode, compression=zipfile.ZIP_DEFLATED)

    for idx, row in df.iterrows():
        if row[md_column] == "" or pd.isnull(row[md_column]):
            md = convert_pdf_to_md(row[url_column], method)
            df.at[idx, md_column] = md

            # Write to zip file
            if zip_file and md_name_column:
                filename = f"{row[md_name_column]}_{method}.md"
                try:
                    zip_file.writestr(filename, md)
                except Exception as e:
                    print(f"⚠️ Failed to write {filename} to ZIP: {e}")

            if csv_output_path:
                df.to_csv(csv_output_path, index=False)

    if zip_file:
        zip_file.close()

    return df
