# opendatabs/pdf_converter

Shared Python utilities and code for ETL pipelines and data projects under the [OpenDataBS](https://github.com/opendatabs) umbrella.

This package is not published on PyPI and is intended to be installed directly from GitHub.

## Installation

You can install this package using either `pip` or [`uv`](https://github.com/astral-sh/uv).

### Install with `uv`

```bash
uv add "git+https://github.com/opendatabs/pdf_converter"
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

## Bulk conversion helpers

`create_markdown_from_column` and `create_text_from_column` convert PDFs from a DataFrame column into a zip archive. Files already in the zip are skipped unless `replace_all=True`. Only successful (non-blank) conversions are written.

Optional parameters for long-running / incremental jobs:

- `max_failures: int | None = None` — after a filename has failed this many times across runs (empty output, download or subprocess failure), skip it on later runs. Failure counts are stored next to the zip as `{zip_stem}.failures.json`. A later successful write clears the entry. `None` (default) retries forever.
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

## Development

To work on this package locally:

```bash
git clone https://github.com/opendatabs/pdf_converter.git
uv sync
source .venv/bin/activate
```

## License

This project is licensed under the MIT License.

