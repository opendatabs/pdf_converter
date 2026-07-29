import logging
import sys
from pathlib import Path

from pdf_converter.backends import (
    EXIT_MISSING_DEPENDENCY,
    EXIT_UNKNOWN_BACKEND,
    MissingBackendDependency,
    UnknownBackend,
)
from pdf_converter.pdf2md import Converter

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s", stream=sys.stderr)

    input_path = Path(sys.argv[1])
    method = sys.argv[2]
    converter = Converter(lib=method, input_file=input_path)
    try:
        converter.convert()
        content = converter.output_file.read_text(encoding="utf-8")
        if converter.last_error:
            print(f"[ERROR] Conversion failed: {converter.last_error}", file=sys.stderr)
            sys.exit(1)
        if not content.strip():
            print("[WARN] Conversion produced no content", file=sys.stderr)
        sys.stdout.write(content)
    except MissingBackendDependency as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(EXIT_MISSING_DEPENDENCY)
    except UnknownBackend as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(EXIT_UNKNOWN_BACKEND)
    except Exception as e:
        print(f"[ERROR] Conversion failed: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        converter.cleanup()
