"""Ingestion tests against a real local HTTP server (no internet, no mocks of requests)."""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Iterator
from datetime import UTC, date, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from src.ingestion import fetch_books
from src.ingestion.open_library_client import ApiResponse, OpenLibraryClient, OpenLibraryError
from src.ingestion.raw_store import write_raw_response

# Odd spacing on purpose: the landing zone must keep the body byte-for-byte.
SAMPLE_BODY = (
    b'{"numFound": 1,  "docs": [{"key": "/works/OL1W", "title": "Example Book",'
    b' "author_name": ["A. Writer"], "isbn": ["9780306406157", "0306406152"]}]}'
)


class _ScriptedHandler(BaseHTTPRequestHandler):
    """Replies according to ``server.script`` (one action per request, last one repeats)."""

    def do_GET(self) -> None:  # noqa: N802 (http.server API)
        server = self.server
        server.seen.append({"path": self.path, "user_agent": self.headers.get("User-Agent")})
        action = server.script.pop(0) if len(server.script) > 1 else server.script[0]
        kind = action[0]
        if kind == "sleep":
            time.sleep(action[1])
            return self._reply(200, SAMPLE_BODY)
        if kind == "status":
            return self._reply(action[1], b'{"error": "scripted"}', {"Retry-After": "0"})
        if kind == "body":
            return self._reply(200, action[1])
        raise AssertionError(f"unknown action {action!r}")

    def _reply(self, status: int, body: bytes, headers: dict[str, str] | None = None) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args: object) -> None:
        pass


@pytest.fixture
def api_server() -> Iterator[ThreadingHTTPServer]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _ScriptedHandler)
    server.script = [("body", SAMPLE_BODY)]
    server.seen = []
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield server
    server.shutdown()
    server.server_close()


def _url(server: ThreadingHTTPServer) -> str:
    return f"http://127.0.0.1:{server.server_address[1]}"


def _client(server: ThreadingHTTPServer, **overrides: object) -> OpenLibraryClient:
    options = {"timeout_seconds": 2.0, "max_retries": 3, "backoff_factor": 0.0, **overrides}
    return OpenLibraryClient(base_url=_url(server), user_agent="test-agent/1.0", **options)


def test_cli_lands_untouched_body_and_metadata(api_server, tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("OPEN_LIBRARY_BASE_URL", _url(api_server))
    monkeypatch.setenv("HTTP_USER_AGENT", "test-agent/1.0")

    exit_code = fetch_books.main(
        ["--subject", "fantasy", "--limit", "2", "--ingestion-date", "2026-10-20"]
    )

    assert exit_code == 0
    landing = tmp_path / "raw/open_library/ingestion_date=2026-10-20/subject=fantasy"
    [body_file] = landing.glob("*.response.json")
    [meta_file] = landing.glob("*.meta.json")
    assert body_file.read_bytes() == SAMPLE_BODY
    meta = json.loads(meta_file.read_text())
    assert meta["subject"] == "fantasy"
    assert meta["record_count"] == 1
    assert meta["http_status"] == 200
    assert meta["request_params"]["limit"] == "2"
    assert len(meta["body_sha256"]) == 64
    assert api_server.seen[0]["user_agent"] == "test-agent/1.0"
    assert "subject=fantasy" in api_server.seen[0]["path"]
    assert json.loads(capsys.readouterr().out)["succeeded"] == ["fantasy"]


def test_subject_underscores_become_spaces_in_query(api_server):
    with _client(api_server) as client:
        response = client.search_by_subject("science_fiction", 5)
    assert "subject=science+fiction" in api_server.seen[0]["path"]
    assert response.params["fields"].startswith("key,title")


def test_retries_transient_errors_then_succeeds(api_server):
    api_server.script = [("status", 503), ("status", 429), ("body", SAMPLE_BODY)]
    with _client(api_server) as client:
        response = client.search_by_subject("history", 1)
    assert response.record_count == 1
    assert len(api_server.seen) == 3


def test_gives_up_after_max_retries(api_server):
    api_server.script = [("status", 503)]
    with _client(api_server, max_retries=2) as client, pytest.raises(OpenLibraryError, match="503"):
        client.search_by_subject("history", 1)
    assert len(api_server.seen) == 3  # first attempt + 2 retries


def test_client_errors_are_not_retried(api_server):
    api_server.script = [("status", 404)]
    with _client(api_server) as client, pytest.raises(OpenLibraryError, match="404"):
        client.search_by_subject("history", 1)
    assert len(api_server.seen) == 1


def test_timeout_raises_after_retries(api_server):
    api_server.script = [("sleep", 1.0)]
    with (
        _client(api_server, timeout_seconds=0.2, max_retries=1) as client,
        pytest.raises(OpenLibraryError, match="failed after retries"),
    ):
        client.search_by_subject("history", 1)
    assert len(api_server.seen) == 2


@pytest.mark.parametrize("body", [b"<html>not json</html>", b'{"unexpected": true}'])
def test_unusable_body_raises(api_server, body):
    api_server.script = [("body", body)]
    with _client(api_server) as client, pytest.raises(OpenLibraryError):
        client.search_by_subject("history", 1)


def test_cli_reports_partial_failure(api_server, tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("OPEN_LIBRARY_BASE_URL", _url(api_server))
    api_server.script = [("body", SAMPLE_BODY), ("status", 404)]

    exit_code = fetch_books.main(
        ["--subject", "fantasy", "--subject", "history", "--pause-seconds", "0"]
    )

    assert exit_code == 1
    assert list((tmp_path / "raw/open_library").glob("*/subject=fantasy/*.response.json"))
    assert not list((tmp_path / "raw/open_library").glob("*/subject=history"))


def _response(fetched_at: datetime) -> ApiResponse:
    return ApiResponse(
        url="http://example.test/search.json",
        params={"subject": "fantasy"},
        status_code=200,
        body=SAMPLE_BODY,
        fetched_at=fetched_at,
        elapsed_ms=5,
        record_count=1,
    )


def test_raw_files_are_never_overwritten(tmp_path: Path):
    fetched_at = datetime(2026, 10, 20, 12, 0, tzinfo=UTC)
    write_raw_response(tmp_path, "fantasy", date(2026, 10, 20), _response(fetched_at))

    with pytest.raises(FileExistsError):
        write_raw_response(tmp_path, "fantasy", date(2026, 10, 20), _response(fetched_at))

    later = datetime(2026, 10, 20, 13, 0, tzinfo=UTC)
    write_raw_response(tmp_path, "fantasy", date(2026, 10, 20), _response(later))
    assert len(list(tmp_path.rglob("*.response.json"))) == 2


@pytest.mark.parametrize("subject", ["../etc", "Fantasy Books", ""])
def test_unsafe_subjects_are_rejected(tmp_path, subject):
    with pytest.raises(ValueError):
        write_raw_response(tmp_path, subject, date(2026, 10, 20), _response(datetime.now(UTC)))


def test_cli_rejects_out_of_range_limit():
    with pytest.raises(SystemExit):
        fetch_books.main(["--subject", "fantasy", "--limit", "0"])
