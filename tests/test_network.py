import sys
import types
import pathlib

# Ensure project root is on sys.path so `Utils` can be imported during tests
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import asyncio
import json
from collections import deque

import pytest


def make_fake_httpx(lines_sequences):
    """Return a fake httpx module where AsyncClient.stream yields sequences of lines.

    lines_sequences: list of lists - each entry is an iterable of lines for one connection.
    """
    class FakeStreamCtx:
        def __init__(self, lines):
            self._lines = list(lines)

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def raise_for_status(self):
            # httpx.Response.raise_for_status is a no-op for 200-like tests
            return None

        async def aiter_lines(self):
            for line in self._lines:
                # simulate network delay
                await asyncio.sleep(0)
                yield line

    class FakeClientCtx:
        def __init__(self, timeout=None):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def stream(self, method, url, headers=None):
            # pop the next sequence to simulate one connection
            if not lines_sequences:
                # empty generator
                return FakeStreamCtx([])
            return FakeStreamCtx(lines_sequences.pop(0))

    fake = types.SimpleNamespace(AsyncClient=FakeClientCtx)
    return fake


def test_fetch_data_success(monkeypatch):
    from Utils import Network

    class Resp:
        status_code = 200

        def json(self):
            return {"ok": True}

    def fake_get(url, headers=None):
        return Resp()

    monkeypatch.setattr(Network, 'requests', types.SimpleNamespace(get=fake_get))

    assert Network.fetch_data() == {"ok": True}


def test_fetch_data_error(monkeypatch):
    from Utils import Network

    class Resp:
        status_code = 500

        def raise_for_status(self):
            raise RuntimeError('bad')

    def fake_get(url, headers=None):
        return Resp()

    monkeypatch.setattr(Network, 'requests', types.SimpleNamespace(get=fake_get))

    with pytest.raises(RuntimeError):
        Network.fetch_data()


@pytest.mark.asyncio
async def test_stream_sse_records_parses_data(monkeypatch):
    # prepare fake httpx that yields a single event then ends
    lines = [
        'data: {"x": 1}',
        '',
    ]
    fake_httpx = make_fake_httpx([lines])
    monkeypatch.setitem(sys.modules, 'httpx', fake_httpx)

    from Utils import Network

    q = deque()
    stop = asyncio.Event()

    task = asyncio.create_task(Network.stream_sse_records(1, q, stop, max_queue_size=10))

    # wait until an item appears
    for _ in range(50):
        if q:
            break
        await asyncio.sleep(0.01)

    assert q, 'no items received'
    item = q.popleft()
    assert isinstance(item, dict)
    assert item.get('x') == 1

    stop.set()
    await task
