import logging
from pathlib import Path

import pandas as pd
import requests
from pdf2md import Converter


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
        return f"Failed to download PDF: {e}"

    # Convert the PDF to Markdown
    converter = Converter(lib=method, input_file=pdf_path)
    try:
        converter.convert()
        markdown_path = converter.output_file
        with open(markdown_path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception as e:
        print(f"Failed to convert PDF to Markdown: {e}")
        return f"Failed to convert PDF to Markdown: {e}"


def add_markdown_column(
    df: pd.DataFrame, url_column: str, method: str, md_column: str = None
) -> pd.DataFrame:
    """
    Adds a column to the DataFrame containing Markdown converted from PDF URLs.

    Args:
        df (pd.DataFrame): The input DataFrame.
        url_column (str): The name of the column containing PDF URLs.
        method (str): The conversion method to use in `convert_pdf_to_md`.
        md_column (str, optional): Name for the new Markdown column. Defaults to '<url_column>_md_<method>'.

    Returns:
        pd.DataFrame: A copy of the DataFrame with an additional column containing Markdown strings.
    """

    def convert_row(pdf_url):
        return convert_pdf_to_md(pdf_url, method)

    if md_column is None:
        md_column = f"{url_column}_md_{method}"
    df[md_column] = df[url_column].apply(convert_row)
    return df
