import logging
import sys
from pathlib import Path

from pdf_converter.pdf2md import Converter

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s", stream=sys.stderr)

    input_path = Path(sys.argv[1])
    method = sys.argv[2]
    converter = Converter(lib=method, input_file=input_path)
    try:
        converter.convert()
        with open(converter.output_file, "r", encoding="utf-8") as f:
            content = f.read()
        if not content.strip():
            reason = converter.last_error or "empty conversion output"
            print(f"[ERROR] Conversion failed: {reason}", file=sys.stderr)
            sys.exit(1)
        print(content)
        sys.exit(0)
    except Exception as e:
        print(f"[ERROR] Conversion failed: {e}", file=sys.stderr)
        sys.exit(1)
