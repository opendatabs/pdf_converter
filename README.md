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

## Development

To work on this package locally:

```bash
git clone https://github.com/opendatabs/pdf_converter.git
uv sync
source .venv/bin/activate
```

## License

This project is licensed under the MIT License.

