import sys
from pathlib import Path

from pdf_converter.backends import (
    EXIT_MISSING_DEPENDENCY,
    EXIT_UNKNOWN_BACKEND,
    MissingBackendDependency,
    UnknownBackend,
)
from pdf_converter.pdf2txt import TextConverter

if __name__ == "__main__":
    input_path = Path(sys.argv[1])
    method = sys.argv[2]
    converter = TextConverter(lib=method, input_file=input_path)
    try:
        converter.convert()
        sys.stdout.write(converter.output_file.read_text(encoding="utf-8"))
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
