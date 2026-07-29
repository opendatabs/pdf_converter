"""Shared helpers for conversion backends.

Every backend lives in its own module and imports its heavy third-party
dependency lazily, through :func:`require`. That keeps ``import pdf_converter``
cheap and lets users install only the extras for the backends they actually use.
"""

import importlib
import logging
from types import ModuleType

logger = logging.getLogger(__name__)


class MissingBackendDependency(ImportError):
    """Raised when a backend is selected but its dependency is not installed."""


def require(module: str, *, backend: str, extra: str) -> ModuleType:
    """Import ``module``, or explain which extra provides it.

    Args:
        module: Importable module name, e.g. ``"pymupdf4llm"``.
        backend: Backend name, used in the error message.
        extra: Name of the packaging extra that installs ``module``.

    Returns:
        The imported module.

    Raises:
        MissingBackendDependency: If the module is not installed.
    """
    try:
        return importlib.import_module(module)
    except ImportError as exc:
        raise MissingBackendDependency(
            f"Backend '{backend}' requires the '{module}' package, which is not installed. "
            f"Install it with:  pip install 'pdf-converter[{extra}]'  "
            f"(or:  uv add 'pdf-converter[{extra}] @ git+https://github.com/opendatabs/pdf_converter')"
        ) from exc


def warn_unused_options(backend: str, options: dict) -> None:
    """Warn that a backend ignores conversion options it does not understand."""
    if options:
        logger.warning("Backend '%s' ignores options: %s", backend, ", ".join(sorted(options)))
