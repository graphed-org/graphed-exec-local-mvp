"""M38 transport conformance suite (spike; frozen at P6). One body, both backends — every
:class:`graphed_core.execution.WorkerTransport` implementation must pass identically, so the
reduction / work-stealing layers above can be written once and run on either."""

from __future__ import annotations

import time

import pytest
from graphed_core.execution import WorkerTransport

from graphed_exec_local._transport import build_ipc_transports, build_transports

ADDRS = ("driver", "w0", "w1", "w2")


@pytest.fixture(params=["ipc", "http"])
def transports(request):
    ts = build_transports(request.param, ADDRS)
    yield ts
    for t in ts.values():
        t.close()


def _wait_recv(t: WorkerTransport, timeout: float = 3.0) -> tuple[str, object] | None:
    """Wait for one message (delivery is async on the HTTP backend)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        got = t.poll()
        if got:
            return got[0]
        time.sleep(0.005)
    return None


def test_satisfies_the_protocol(transports) -> None:
    for addr, t in transports.items():
        assert isinstance(t, WorkerTransport)  # structural conformance (runtime_checkable)
        assert t.address == addr
        assert set(t.peers()) == set(ADDRS) - {addr}


def test_directed_send_and_isolation(transports) -> None:
    assert transports["w0"].send("w1", ("hello", 1)) is True
    got = _wait_recv(transports["w1"])
    assert got == ("w0", ("hello", 1))  # sender is tagged, payload intact
    # isolation: the message went to w1 only
    assert transports["w2"].poll() == []
    assert transports["driver"].poll() == []


def test_recv_with_timeout(transports) -> None:
    assert transports["driver"].recv(timeout=0.05) is None  # nothing yet -> None, no hang
    transports["w2"].send("driver", "ready")
    msg = None
    deadline = time.monotonic() + 3.0
    while msg is None and time.monotonic() < deadline:
        msg = transports["driver"].recv(timeout=0.2)
    assert msg == ("w2", "ready")


def test_broadcast_reaches_every_peer_but_not_self(transports) -> None:
    transports["driver"].broadcast("ping")
    for w in ("w0", "w1", "w2"):
        assert _wait_recv(transports[w]) == ("driver", "ping")
    assert transports["driver"].poll() == []  # never delivers to itself


def test_unknown_destination_is_a_no_op(transports) -> None:
    assert transports["w0"].send("nobody", "x") is False
    assert transports["w0"].send("w0", "self") is False  # cannot send to self


def test_close_is_safe_and_idempotent(transports) -> None:
    transports["w0"].close()
    transports["w0"].close()  # idempotent
    # a send after the destination closed must not raise (best-effort)
    transports["w1"].send("w0", "after-close")


def test_ipc_drops_on_a_full_inbox() -> None:
    # backend-specific: the IPC inbox is bounded -> send returns False (drops) instead of blocking.
    tiny = build_ipc_transports(("driver", "w0"), maxsize=2)
    try:
        assert tiny["driver"].send("w0", 1) is True
        assert tiny["driver"].send("w0", 2) is True
        assert tiny["driver"].send("w0", 3) is False  # full -> dropped, never blocks
    finally:
        for t in tiny.values():
            t.close()
