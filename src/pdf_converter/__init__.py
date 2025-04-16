import requests
import logging

from pdf_converter.pdf2md import Converter

def convert_pdf_to_md(pdf_url, pdf_path, prefix):
    # Download the PDF
    logging.info(f"   Downloading PDF: {pdf_url}")
    try:
        r_pdf = requests.get(pdf_url)
        with open(pdf_path, "wb") as file:
            for chunk in r_pdf.iter_content(chunk_size=1024):
                file.write(chunk)
        print(f"PDF downloaded successfully as {pdf_path}")
    except:
        print("Failed to download PDF.")
        return dict()

    # Convert the PDF to Markdown
    methods = ["docling", "pymupdf4llm", "pymupdf"]
    markdowns = {}
    for m in methods:
        converter = Converter(lib=m, input_file=pdf_path)
        try:
            converter.convert()
            markdown_path = converter.output_file
            with open(markdown_path, "r", encoding="utf-8") as f:
                markdowns[f'{prefix}_{m}'] = f.read()
        except:
            logging.error(f"Failed to convert PDF using method: {m}")
            continue

    return markdowns