# opendatabs/pdf_converter

Shared Python utilities and code for ETL pipelines and data projects under the [OpenDataBS](https://github.com/opendatabs) umbrella.

This package is not published on PyPI and is intended to be installed directly from GitHub.

## Installation

You can install this package using either `pip` or [`uv`](https://github.com/astral-sh/uv).

Each conversion method lives in its own module behind its own **extra**, so you
only install the dependencies for the methods you actually use. The base install
pulls in nothing heavier than `pandas`, `requests` and `tqdm`.

| Method (`method=`) | Extra | Pulls in | Module |
| --- | --- | --- | --- |
| `docling-serve` | `docling-serve` | `httpx` | `backends/docling_serve.py` |
| `docling` | `docling` | `docling` (+ torch, models) | `backends/docling_local.py` |
| `pymupdf4llm` | `pymupdf4llm` | `pymupdf4llm` | `backends/pymupdf4llm_backend.py` |
| `pymupdf` | `pymupdf` | `pymupdf` | `backends/pymupdf_backend.py` |
| `pdfplumber` | `pdfplumber` | `pdfplumber` | `backends/pdfplumber_backend.py` |
| _(image extraction)_ | `images` | `pymupdf`, `pillow` | `backends/images.py` |
| _(everything)_ | `all` | all of the above | — |

Selecting one backend never imports another's dependencies, and picking a method
whose extra is missing raises a `MissingBackendDependency` naming the extra to install.
An unrecognised method name raises `UnknownBackend` rather than falling back to a
default backend — a fallback would store one backend's output under another's filename.
Note that `docling-serve`, `docling` and `pymupdf4llm` are markdown-only: passing them
to the text helpers raises `UnknownBackend`.

Recommended for the remote Docling Serve service — `httpx` only, no local model stack:

```bash
uv add "pdf-converter[docling-serve] @ git+https://github.com/opendatabs/pdf_converter"
```

### Install with `uv`

```bash
uv add "git+https://github.com/opendatabs/pdf_converter"

# with extras
uv add "pdf-converter[docling-serve,pymupdf] @ git+https://github.com/opendatabs/pdf_converter"
```

You can also install a specific tag, commit, or branch:

```bash
# Install from a specific tag
uv add "git+https://github.com/opendatabs/pdf_converter@v0.1.0"

# Install from a specific commit
uv add "git+https://github.com/opendatabs/pdf_converter@<commit-sha>"

# Install from a branch (e.g., main)
uv add "git+https://github.com/opendatabs/pdf_converter@main"
```

### Install with `pip`

```bash
pip install "git+https://github.com/opendatabs/pdf_converter"

# with extras
pip install "pdf-converter[docling-serve] @ git+https://github.com/opendatabs/pdf_converter"
```

As with `uv`, you can install a specific reference:

```bash
# From a tag
pip install "git+https://github.com/opendatabs/pdf_converter@v0.1.0"

# From a commit
pip install "git+https://github.com/opendatabs/pdf_converter@<commit-sha>"

# From a branch
pip install "git+https://github.com/opendatabs/pdf_converter@main"
```

## Docling Serve

`method="docling-serve"` converts against a remote [Docling Serve](https://github.com/docling-project/docling-serve)
instance using its asynchronous API: one file is submitted to
`POST /v1/convert/file/async`, its `task_id` is polled at `GET /v1/status/poll/{task_id}`
until the task is terminal, then the markdown is fetched from `GET /v1/result/{task_id}`.

Configure it with two environment variables (a `.env` file is read automatically):

```bash
DOCLING_HTTP_CLIENT=https://docling.internal.example/   # required
DOCLING_API_KEY=...                                     # optional; omitted = no auth header
```

Unlike the other methods this one runs in-process rather than in a subprocess —
there is no local library to crash, so a subprocess would only add startup cost.

Notes on the polling loop:

- Polling paces itself with an explicit `poll_interval` sleep. The server's `wait`
  query parameter is sent as a hint only, because docling-serve does not reliably
  honour it ([docling-serve#388](https://github.com/docling-project/docling-serve/issues/388));
  depending on it turns the loop into a busy loop against the server.
- Transient poll failures (connection errors, timeouts, 5xx, and 404s within a
  60s grace window after submit — see [docling-serve#467](https://github.com/docling-project/docling-serve/issues/467))
  are retried instead of failing a job that may already be an hour in.
- Queue time and conversion time have separate budgets (`queue_timeout` and
  `document_timeout`), so a long server queue cannot eat the conversion budget.
  `document_timeout` restarts once the task is first reported as started.

For a single file you can bypass the batch helpers entirely:

```python
from pathlib import Path
from pdf_converter.docling_client import convert_file_to_markdown

md = convert_file_to_markdown(Path("report.pdf"), poll_interval=5.0, document_timeout=3600)
```

TLS certificates are verified by default. For an internally hosted instance with a
private CA, pass the CA bundle path (`verify="/path/to/ca.pem"`) rather than disabling
verification; `verify=False` is still accepted as a last resort.

## Bulk conversion helpers

`create_markdown_from_column` and `create_text_from_column` convert PDFs from a DataFrame column into a zip archive. Files already in the zip are skipped unless `replace_all=True`. Only successful conversions are written, where success is decided by the backend and not by output length: a document that legitimately converts to nothing is written as an empty file so it is not retried on every run.

A missing backend extra or an unknown method name aborts the whole run instead of counting as a per-document failure.

Optional parameters for long-running / incremental jobs:

- `max_failures: int | None = None` — after a filename has failed this many times across runs (download, conversion or zip-write failure), skip it on later runs. Failure counts are stored next to the zip as `{zip_stem}.failures.json`. A later successful write clears the entry. `None` (default) retries forever.
- `shuffle: bool = False` — shuffle remaining work after filtering existing files and exhausted failures, so each run tries remaining documents in a different order (helps with transient / rate-limit failures).
- `max_workers: int = 1` — number of parallel conversions (`1` = sequential). Especially useful with `method="docling-serve"`; start with 2–4 and raise carefully until you know the server’s limit. Zip writes and failure-count updates stay serial.

Example:

```python
from pathlib import Path
from pdf_converter import create_markdown_from_column

create_markdown_from_column(
    df,
    url_column="pdf_url",
    method="docling-serve",
    zip_path=Path("output/markdown.zip"),
    md_name_column="title",
    max_failures=3,
    shuffle=True,
    max_workers=4,
)
```

## Architecture & Execution Model

- **Subprocess Crash Isolation**: Local conversion backends (`docling`, `pymupdf`, `pymupdf4llm`, `pdfplumber`) run inside a subprocess per document. This ensures heavy C-libraries or memory leaks in PDF parsers do not crash the primary orchestration process.
- **In-Process Remote Backend**: Remote backends like `docling-serve` run in-process using `httpx`, avoiding subprocess overhead since conversion logic runs on the remote server.
- **CLI Helper Scripts**: `convert_single_pdf2md.py` and `convert_single_pdf2txt.py` write converted content byte-for-byte to `stdout` and logs/errors to `stderr`. Dedicated exit codes (`3` for missing dependencies, `4` for unknown backends) signal misconfiguration to abort batch processing immediately instead of consuming per-document failure retry budgets.

## Backend Heuristics & Features

- **`pymupdf`**: Infers Markdown headings based on font size thresholds (`H1 >= 18pt`, `H2 >= 16pt`, `H3 >= 14pt`) and formatting flags (bold).
- **`pdfplumber`**: Infers headings when a word's font size is > 1.2× the page average font size, and formats extracted tables into Markdown tables.
- **`images`**: Extracts embedded raster images from PDF pages and saves them as PNG files.

## Development

To work on this package locally:

```bash
git clone https://github.com/opendatabs/pdf_converter.git
uv sync
source .venv/bin/activate
```

## License

This project is licensed under the MIT License.

