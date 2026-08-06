"""Standalone client for the Docling Serve asynchronous conversion API.

Implements the submit -> poll -> fetch-result flow against a Docling Serve
instance:

1. ``POST /v1/convert/source/async`` returns a ``task_id`` immediately.
   Sources are either an HTTP URL (``kind=http``) or a base64 file
   (``kind=file``) — both as JSON, never multipart.
2. ``GET  /v1/status/poll/{task_id}`` is polled until the task is terminal.
3. ``GET  /v1/result/{task_id}`` returns the converted document.

This module deliberately imports nothing from ``docling`` itself. Callers that
only use the remote service therefore never pay the (multi-second, multi-GB)
cost of loading the local conversion stack.

Polling paces itself with an explicit sleep. The server-side ``wait`` query
parameter is sent as a best-effort hint only, because docling-serve does not
reliably honour it (docling-serve#388) — relying on it turns the loop into a
busy loop against the server.
"""

import base64
import io
import logging
import os
import time
import zipfile
from pathlib import Path
from typing import Any

import httpx
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

TERMINAL_TASK_STATUSES = frozenset({"success", "failure", "partial_success", "skipped"})
SUCCESS_TASK_STATUSES = frozenset({"success", "partial_success"})
STARTED_TASK_STATUSES = frozenset({"started", "running", "processing"})

DEFAULT_DOCUMENT_TIMEOUT_SECONDS = 3600.0
DEFAULT_QUEUE_TIMEOUT_SECONDS = 3600.0
DEFAULT_POLL_INTERVAL_SECONDS = 5.0
DEFAULT_POLL_WAIT_SECONDS = 30.0
DEFAULT_SUBMIT_TIMEOUT_SECONDS = 120.0
DEFAULT_RESULT_TIMEOUT_SECONDS = 120.0

_POLL_TIMEOUT_BUFFER_SECONDS = 15.0
_POLL_NOT_FOUND_GRACE_SECONDS = 60.0
_TRANSIENT_STATUS_CODES = frozenset({408, 425, 429, 500, 502, 503, 504})


class DoclingServeError(RuntimeError):
    """Raised when a Docling Serve conversion cannot be completed."""


def _describe_http_error(exc: BaseException) -> str:
    """Build a readable error string, including HTTP response body when available."""
    if isinstance(exc, httpx.TimeoutException):
        cause = f"{type(exc).__name__}: {exc}"
        if exc.__cause__ is not None:
            cause = f"{cause} (cause={exc.__cause__!r})"
        return cause
    if isinstance(exc, httpx.HTTPStatusError):
        return f"{exc} | response={_describe_response(exc.response)}"
    return f"{type(exc).__name__}: {exc}"


def _describe_response(response: httpx.Response) -> str:
    try:
        body = (response.text or "").strip().replace("\n", " ")
    except Exception:  # pragma: no cover - binary/undecodable body
        body = repr(response.content[:200])
    return f"{body[:500]}..." if len(body) > 500 else body


def get_base_url() -> str:
    """Return the configured Docling Serve base URL without a trailing slash."""
    base_url = os.getenv("DOCLING_HTTP_CLIENT")
    if not base_url:
        raise DoclingServeError("DOCLING_HTTP_CLIENT is not set.")
    return base_url.rstrip("/")


def get_headers() -> dict[str, str]:
    """Return the auth headers for Docling Serve.

    ``DOCLING_API_KEY`` is optional: internally hosted instances are often
    unauthenticated. When it is unset, no Authorization header is sent.
    """
    api_key = os.getenv("DOCLING_API_KEY")
    if not api_key:
        logger.warning("DOCLING_API_KEY is not set; sending requests without an Authorization header")
        return {}
    return {"Authorization": f"Bearer {api_key}"}


def build_convert_options(
    *,
    to_formats: list[str] | None = None,
    image_export_mode: str = "embedded",
    pipeline: str = "standard",
    do_ocr: bool = True,
    force_ocr: bool = False,
    ocr_engine: str = "easyocr",
    ocr_lang: list[str] | None = None,
    pdf_backend: str = "pypdfium2",
    table_mode: str = "accurate",
    abort_on_error: bool = False,
    include_images: bool = True,
    images_scale: float = 2,
    md_page_break_placeholder: str = "",
    page_range: list[int] | None = None,
    document_timeout: float = DEFAULT_DOCUMENT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Build the ``options`` object for a ``ConvertDocumentsRequest``."""
    options: dict[str, Any] = {
        "to_formats": to_formats if to_formats is not None else ["md"],
        "document_timeout": document_timeout,
        "include_images": include_images,
        "image_export_mode": image_export_mode,
        "images_scale": images_scale,
        "md_page_break_placeholder": md_page_break_placeholder,
        "pipeline": pipeline,
        "do_ocr": do_ocr,
        "force_ocr": force_ocr,
        "ocr_engine": ocr_engine,
        "ocr_lang": ocr_lang if ocr_lang is not None else ["en", "fr", "de", "it"],
        "pdf_backend": pdf_backend,
        "table_mode": table_mode,
        "abort_on_error": abort_on_error,
    }
    if page_range:
        options["page_range"] = page_range
    return options


def build_source_payload(
    *,
    source_url: str | None = None,
    input_file: Path | None = None,
    source_headers: dict[str, str] | None = None,
    return_as_file: bool = False,
    document_timeout: float = DEFAULT_DOCUMENT_TIMEOUT_SECONDS,
    **conversion_options: Any,
) -> dict[str, Any]:
    """Build a ``ConvertDocumentsRequest`` body for ``/v1/convert/source/async``.

    Provide either ``source_url`` (HTTP fetch by Docling Serve) or ``input_file``
    (base64 ``FileSourceRequest``). URL is preferred when both are given.
    """
    if source_url:
        source: dict[str, Any] = {"kind": "http", "url": source_url}
        if source_headers:
            source["headers"] = source_headers
        source_label = source_url
    elif input_file is not None:
        encoded = base64.b64encode(input_file.read_bytes()).decode("ascii")
        source = {
            "kind": "file",
            "filename": input_file.name,
            "base64_string": encoded,
        }
        source_label = input_file.name
    else:
        raise DoclingServeError("Either source_url or input_file is required.")

    return_as_file = bool(conversion_options.pop("return_as_file", return_as_file))
    payload = {
        "options": build_convert_options(document_timeout=document_timeout, **conversion_options),
        "sources": [source],
        "target": {"kind": "zip" if return_as_file else "inbody"},
    }
    logger.debug("Built Docling source payload for %s (kind=%s)", source_label, source["kind"])
    return payload


def submit_source_task(
    client: httpx.Client,
    payload: dict[str, Any],
    *,
    base_url: str,
    headers: dict[str, str],
    timeout: float = DEFAULT_SUBMIT_TIMEOUT_SECONDS,
) -> str:
    """Submit a JSON ``ConvertDocumentsRequest`` to ``/v1/convert/source/async``.

    Args:
        client: Shared httpx client.
        payload: Request body from :func:`build_source_payload`.
        base_url: Docling Serve base URL without trailing slash.
        headers: Request headers including authorization.
        timeout: HTTP timeout for the submit request in seconds.

    Returns:
        The task id assigned by Docling Serve.

    Raises:
        DoclingServeError: If the submit fails or the response has no task_id.
    """
    url = f"{base_url}/v1/convert/source/async"
    timeout_config = httpx.Timeout(connect=30.0, read=timeout, write=timeout, pool=30.0)
    source = (payload.get("sources") or [{}])[0]
    logger.info(
        "Submitting Docling async source job (kind=%s) to %s",
        source.get("kind", "unknown"),
        url,
    )
    started = time.monotonic()
    try:
        response = client.post(url, headers=headers, json=payload, timeout=timeout_config)
    except httpx.TimeoutException as exc:
        elapsed = time.monotonic() - started
        raise DoclingServeError(
            f"submit timed out after {elapsed:.1f}s (configured {timeout:.0f}s). The async endpoint "
            f"should return a task_id within seconds — Docling Serve may be blocked or a proxy is "
            f"killing the request. Detail: {_describe_http_error(exc)}"
        ) from exc
    except httpx.HTTPError as exc:
        elapsed = time.monotonic() - started
        raise DoclingServeError(f"submit failed after {elapsed:.1f}s to {url}: {_describe_http_error(exc)}") from exc

    if response.status_code >= 400:
        raise DoclingServeError(f"submit HTTP {response.status_code} from {url}: {_describe_response(response)}")

    try:
        body = response.json()
    except ValueError as exc:
        raise DoclingServeError(f"submit response is not JSON: {_describe_response(response)}") from exc

    task_id = body.get("task_id")
    if not task_id:
        raise DoclingServeError(f"submit response missing task_id: {body}")
    return str(task_id)


def poll_task(
    client: httpx.Client,
    task_id: str,
    *,
    base_url: str,
    headers: dict[str, str],
    poll_interval: float = DEFAULT_POLL_INTERVAL_SECONDS,
    poll_wait: float = DEFAULT_POLL_WAIT_SECONDS,
    queue_timeout: float = DEFAULT_QUEUE_TIMEOUT_SECONDS,
    document_timeout: float = DEFAULT_DOCUMENT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Poll ``/v1/status/poll/{task_id}`` until the task reaches a terminal status.

    Two independent budgets are used so a long server-side queue cannot consume
    the conversion budget: ``queue_timeout`` covers the wait until the task
    starts, ``document_timeout`` covers the conversion itself and starts over
    once the task is first reported as started.

    Transient failures (connection errors, request timeouts, 5xx, and 404s
    within a short grace period after submit) are retried until the deadline;
    only the deadline or an unambiguous client error fails the conversion.

    Args:
        client: Shared httpx client.
        task_id: Async task id returned by :func:`submit_source_task`.
        base_url: Docling Serve base URL without trailing slash.
        headers: Request headers including authorization.
        poll_interval: Seconds slept between polls.
        poll_wait: Best-effort server-side long-poll hint, in seconds.
        queue_timeout: Max seconds to wait for the task to start.
        document_timeout: Max seconds to wait for a started task to finish.

    Returns:
        The final task status payload.

    Raises:
        DoclingServeError: On deadline expiry or a fatal HTTP error.
    """
    url = f"{base_url}/v1/status/poll/{task_id}"
    now = time.monotonic()
    deadline = now + queue_timeout
    not_found_deadline = now + _POLL_NOT_FOUND_GRACE_SECONDS
    seen_started = False
    last_status = ""

    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            budget = (
                f"document_timeout={document_timeout:.0f}s" if seen_started else f"queue_timeout={queue_timeout:.0f}s"
            )
            raise DoclingServeError(
                f"task {task_id} did not finish in time (last status={last_status or 'unknown'}; exceeded {budget})"
            )

        wait = max(0.0, min(poll_wait, remaining))
        try:
            response = client.get(
                url,
                headers=headers,
                params={"wait": wait},
                timeout=wait + _POLL_TIMEOUT_BUFFER_SECONDS,
            )
        except httpx.TransportError as exc:
            logger.warning(
                "Docling poll for task %s failed transiently (%s); retrying (%.0fs left)",
                task_id,
                _describe_http_error(exc),
                deadline - time.monotonic(),
            )
            time.sleep(min(poll_interval, max(0.0, deadline - time.monotonic())))
            continue
        except httpx.HTTPError as exc:
            raise DoclingServeError(f"poll failed for task {task_id}: {_describe_http_error(exc)}") from exc

        if response.status_code == 404 and time.monotonic() < not_found_deadline:
            logger.warning("Docling task %s not found yet; retrying within grace period", task_id)
            time.sleep(min(poll_interval, max(0.0, deadline - time.monotonic())))
            continue
        if response.status_code in _TRANSIENT_STATUS_CODES:
            logger.warning(
                "Docling poll for task %s returned HTTP %s; retrying (%.0fs left)",
                task_id,
                response.status_code,
                deadline - time.monotonic(),
            )
            time.sleep(min(poll_interval, max(0.0, deadline - time.monotonic())))
            continue
        if response.status_code >= 400:
            raise DoclingServeError(
                f"poll HTTP {response.status_code} for task {task_id}: {_describe_response(response)}"
            )

        try:
            payload: dict[str, Any] = response.json()
        except ValueError:
            logger.warning("Docling poll for task %s returned non-JSON body; retrying", task_id)
            time.sleep(min(poll_interval, max(0.0, deadline - time.monotonic())))
            continue

        last_status = str(payload.get("task_status", ""))
        if last_status in TERMINAL_TASK_STATUSES:
            logger.info("Docling task %s finished with status=%s", task_id, last_status)
            return payload

        if not seen_started and last_status in STARTED_TASK_STATUSES:
            seen_started = True
            deadline = time.monotonic() + document_timeout
            logger.info("Docling task %s started; conversion budget %.0fs", task_id, document_timeout)

        logger.info(
            "Docling task %s status=%s position=%s (%.0fs left)",
            task_id,
            last_status or "unknown",
            payload.get("task_position"),
            deadline - time.monotonic(),
        )
        time.sleep(min(poll_interval, max(0.0, deadline - time.monotonic())))


def _markdown_from_zip(content: bytes, task_id: str) -> str:
    """Extract the first markdown member from a zip result payload."""
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        names = [name for name in archive.namelist() if name.lower().endswith(".md")]
        if not names:
            raise DoclingServeError(f"task {task_id} zip result contains no .md file: {archive.namelist()}")
        return archive.read(names[0]).decode("utf-8")


def fetch_result(
    client: httpx.Client,
    task_id: str,
    *,
    base_url: str,
    headers: dict[str, str],
    timeout: float = DEFAULT_RESULT_TIMEOUT_SECONDS,
    return_as_file: bool = False,
) -> str:
    """Fetch the markdown content of a finished task from ``/v1/result/{task_id}``.

    Args:
        client: Shared httpx client.
        task_id: Completed async task id.
        base_url: Docling Serve base URL without trailing slash.
        headers: Request headers including authorization.
        timeout: HTTP timeout for the result request in seconds.
        return_as_file: Must match the submitted ``target_type``; when True the
            body is a zip archive rather than JSON.

    Returns:
        Markdown content. An empty string means the server converted the
        document to no text; a *missing* ``md_content`` is an error, not an
        empty document.

    Raises:
        DoclingServeError: If the result is missing, malformed or unsuccessful.
    """
    url = f"{base_url}/v1/result/{task_id}"
    timeout_config = httpx.Timeout(connect=30.0, read=timeout, write=timeout, pool=30.0)
    try:
        response = client.get(url, headers=headers, timeout=timeout_config)
    except httpx.HTTPError as exc:
        raise DoclingServeError(f"result fetch failed for task {task_id}: {_describe_http_error(exc)}") from exc

    if response.status_code >= 400:
        raise DoclingServeError(
            f"result HTTP {response.status_code} for task {task_id}: {_describe_response(response)}"
        )

    if return_as_file:
        return _markdown_from_zip(response.content, task_id)

    try:
        result: dict[str, Any] = response.json()
    except ValueError as exc:
        raise DoclingServeError(f"result for task {task_id} is not JSON: {_describe_response(response)}") from exc

    status = result.get("status")
    if status not in SUCCESS_TASK_STATUSES:
        raise DoclingServeError(f"task {task_id} result unsuccessful: status={status}, errors={result.get('errors')}")

    document = result.get("document")
    if not isinstance(document, dict):
        raise DoclingServeError(f"task {task_id} result document is not a dict: {document!r}")

    if document.get("md_content") is None:
        raise DoclingServeError(
            f"task {task_id} result has no md_content (keys={sorted(document)}). "
            f"The server did not return markdown — check that 'md' is in to_formats."
        )
    md_content = document["md_content"]
    if not isinstance(md_content, str):
        raise DoclingServeError(f"task {task_id} md_content is not a string: {type(md_content)!r}")
    return md_content


def convert_file_to_markdown(
    input_file: Path | None = None,
    *,
    source_url: str | None = None,
    source_headers: dict[str, str] | None = None,
    poll_interval: float = DEFAULT_POLL_INTERVAL_SECONDS,
    poll_wait: float = DEFAULT_POLL_WAIT_SECONDS,
    queue_timeout: float = DEFAULT_QUEUE_TIMEOUT_SECONDS,
    document_timeout: float = DEFAULT_DOCUMENT_TIMEOUT_SECONDS,
    submit_timeout: float = DEFAULT_SUBMIT_TIMEOUT_SECONDS,
    result_timeout: float = DEFAULT_RESULT_TIMEOUT_SECONDS,
    verify: bool | str = True,
    **conversion_options: Any,
) -> str:
    """Convert a PDF to markdown via the Docling Serve async source API.

    Submits to ``/v1/convert/source/async`` (HTTP URL or base64 file), polls the
    task id until terminal, then fetches the result. One document, one task,
    one blocking call.

    Args:
        input_file: Local PDF to send as a base64 ``FileSourceRequest``. Ignored
            when ``source_url`` is set.
        source_url: HTTP(S) URL for Docling Serve to fetch (``HttpSourceRequest``).
        source_headers: Optional headers Docling Serve should use when fetching
            ``source_url`` (e.g. authorization for the PDF host).
        poll_interval: Seconds slept between status polls.
        poll_wait: Best-effort server-side long-poll hint, in seconds.
        queue_timeout: Max seconds to wait for the task to start.
        document_timeout: Max seconds for the conversion once started. Also sent
            to the server as its own per-document timeout.
        submit_timeout: HTTP timeout for the submit request.
        result_timeout: HTTP timeout for the result fetch.
        verify: TLS verification for the HTTP client. Certificates are verified
            by default; pass a CA bundle path for a privately signed instance,
            or ``False`` to disable verification entirely (last resort).
        **conversion_options: Forwarded to :func:`build_convert_options`.

    Returns:
        Markdown content. May be an empty string if the document has no text.

    Raises:
        DoclingServeError: If configuration is missing or conversion fails.
    """
    base_url = get_base_url()
    headers = get_headers()
    return_as_file = bool(conversion_options.get("return_as_file", False))
    payload = build_source_payload(
        source_url=source_url,
        input_file=input_file,
        source_headers=source_headers,
        document_timeout=document_timeout,
        **conversion_options,
    )
    label = source_url or (input_file.name if input_file is not None else "unknown")

    with httpx.Client(verify=verify, timeout=None) as client:
        task_id = submit_source_task(
            client,
            payload,
            base_url=base_url,
            headers=headers,
            timeout=submit_timeout,
        )
        logger.info("Submitted Docling task %s for %s", task_id, label)

        status_payload = poll_task(
            client,
            task_id,
            base_url=base_url,
            headers=headers,
            poll_interval=poll_interval,
            poll_wait=poll_wait,
            queue_timeout=queue_timeout,
            document_timeout=document_timeout,
        )
        task_status = str(status_payload.get("task_status", ""))
        if task_status not in SUCCESS_TASK_STATUSES:
            raise DoclingServeError(
                f"task {task_id} failed: status={task_status}, error_message={status_payload.get('error_message')}"
            )

        return fetch_result(
            client,
            task_id,
            base_url=base_url,
            headers=headers,
            timeout=result_timeout,
            return_as_file=return_as_file,
        )
