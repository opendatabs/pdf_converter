"""Shared helpers for conversion backends.

Every backend lives in its own module and imports its heavy third-party
dependency lazily, through :func:`require`. That keeps ``import pdf_converter``
cheap and lets users install only the extras for the backends they actually use.
"""

import importlib
import logging
from types import ModuleType

logger = logging.getLogger(__name__)

EXIT_MISSING_DEPENDENCY = 3
EXIT_UNKNOWN_BACKEND = 4


class MissingBackendDependency(ImportError):
    """Raised when a backend is selected but its dependency is not installed."""


class UnknownBackend(ValueError):
    """Raised when a method name does not match any registered backend."""


def require(module: str, *, backend: str, extra: str) -> ModuleType:
    """Import ``module``, or explain which extra provides it.

    Only a genuinely absent ``module`` is reported as a missing extra. An import
    error raised from *inside* an installed package propagates unchanged, so a
    broken dependency is not mistaken for an uninstalled one.

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
    except ModuleNotFoundError as exc:
        if exc.name is None or not (module == exc.name or module.startswith(f"{exc.name}.")):
            raise
        raise MissingBackendDependency(
            f"Backend '{backend}' requires the '{module}' package, which is not installed. "
            f"Install it with:  pip install 'pdf-converter[{extra}]'  "
            f"(or:  uv add 'pdf-converter[{extra}] @ git+https://github.com/opendatabs/pdf_converter')"
        ) from exc


def warn_unused_options(backend: str, options: dict) -> None:
    """Warn that a backend ignores conversion options it does not understand."""
    if options:
        logger.warning("Backend '%s' ignores options: %s", backend, ", ".join(sorted(options)))
