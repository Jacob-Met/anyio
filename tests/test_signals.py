from __future__ import annotations

import asyncio
import os
import signal
import sys
from collections.abc import AsyncIterable

import pytest

from anyio import create_task_group, fail_after, open_signal_receiver, to_thread

pytestmark = [
    pytest.mark.skipif(
        sys.platform == "win32",
        reason="Signal delivery cannot be tested on Windows",
    ),
    # Workaround for https://github.com/MagicStack/uvloop/issues/703
    pytest.mark.filterwarnings(
        "ignore:'asyncio.iscoroutinefunction' is deprecated:DeprecationWarning"
    ),
]


async def test_receive_signals() -> None:
    with open_signal_receiver(signal.SIGUSR1, signal.SIGUSR2) as sigiter:
        await to_thread.run_sync(os.kill, os.getpid(), signal.SIGUSR1)
        await to_thread.run_sync(os.kill, os.getpid(), signal.SIGUSR2)
        with fail_after(1):
            sigusr1 = await sigiter.__anext__()
            assert isinstance(sigusr1, signal.Signals)
            assert sigusr1 == signal.Signals.SIGUSR1

            sigusr2 = await sigiter.__anext__()
            assert isinstance(sigusr2, signal.Signals)
            assert sigusr2 == signal.Signals.SIGUSR2


async def test_task_group_cancellation_open() -> None:
    async def signal_handler() -> None:
        with open_signal_receiver(signal.SIGUSR1) as sigiter:
            async for _ in sigiter:
                pytest.fail("SIGUSR1 should not be sent")

            pytest.fail("signal_handler should have been cancelled")

        pytest.fail("open_signal_receiver should not suppress cancellation")

    async with create_task_group() as tg:
        tg.start_soon(signal_handler)
        tg.cancel_scope.cancel()


async def test_task_group_cancellation_consume() -> None:
    async def consume(sigiter: AsyncIterable[int]) -> None:
        async for _ in sigiter:
            pytest.fail("SIGUSR1 should not be sent")

        pytest.fail("consume should have been cancelled")

    with open_signal_receiver(signal.SIGUSR1) as sigiter:
        async with create_task_group() as tg:
            tg.start_soon(consume, sigiter)
            tg.cancel_scope.cancel()


async def test_restore_previous_python_signal_handler(anyio_backend_name: str) -> None:
    if anyio_backend_name != "asyncio":
        pytest.skip("asyncio backend regression")

    previous = signal.getsignal(signal.SIGUSR1)
    delivered: list[int] = []

    def custom_handler(signum: int, frame: object) -> None:
        delivered.append(signum)

    signal.signal(signal.SIGUSR1, custom_handler)
    try:
        with open_signal_receiver(signal.SIGUSR1):
            assert signal.getsignal(signal.SIGUSR1) is not custom_handler

        assert signal.getsignal(signal.SIGUSR1) is custom_handler
        os.kill(os.getpid(), signal.SIGUSR1)
        assert delivered == [signal.SIGUSR1]
    finally:
        signal.signal(signal.SIGUSR1, previous)


async def test_restore_ignored_signal(anyio_backend_name: str) -> None:
    if anyio_backend_name != "asyncio":
        pytest.skip("asyncio backend regression")

    previous = signal.getsignal(signal.SIGUSR1)
    signal.signal(signal.SIGUSR1, signal.SIG_IGN)
    try:
        with open_signal_receiver(signal.SIGUSR1):
            assert signal.getsignal(signal.SIGUSR1) != signal.SIG_IGN

        assert signal.getsignal(signal.SIGUSR1) == signal.SIG_IGN
    finally:
        signal.signal(signal.SIGUSR1, previous)


async def test_failed_signal_registration_restores_handler(
    anyio_backend_name: str,
) -> None:
    if anyio_backend_name != "asyncio":
        pytest.skip("asyncio backend regression")

    previous = signal.getsignal(signal.SIGUSR1)

    def custom_handler(signum: int, frame: object) -> None:
        pass

    signal.signal(signal.SIGUSR1, custom_handler)
    try:
        with pytest.raises((RuntimeError, OSError)):
            with open_signal_receiver(signal.SIGUSR1, signal.SIGKILL):
                pass

        assert signal.getsignal(signal.SIGUSR1) is custom_handler
    finally:
        signal.signal(signal.SIGUSR1, previous)


async def test_preserve_asyncio_loop_signal_handler(anyio_backend_name: str) -> None:
    if anyio_backend_name != "asyncio":
        pytest.skip("asyncio-specific signal callback")

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGUSR1, lambda: None)
    try:
        with open_signal_receiver(signal.SIGUSR1) as receiver:
            await to_thread.run_sync(os.kill, os.getpid(), signal.SIGUSR1)
            with fail_after(1):
                assert await receiver.__anext__() == signal.SIGUSR1

        assert loop.remove_signal_handler(signal.SIGUSR1)
    finally:
        loop.remove_signal_handler(signal.SIGUSR1)


async def test_nested_asyncio_signal_receivers(anyio_backend_name: str) -> None:
    if anyio_backend_name != "asyncio":
        pytest.skip("asyncio-specific signal callback")

    with open_signal_receiver(signal.SIGUSR1) as outer:
        with open_signal_receiver(signal.SIGUSR1) as inner:
            await to_thread.run_sync(os.kill, os.getpid(), signal.SIGUSR1)
            with fail_after(1):
                assert await inner.__anext__() == signal.SIGUSR1

        assert signal.getsignal(signal.SIGUSR1) != signal.SIG_DFL
        await to_thread.run_sync(os.kill, os.getpid(), signal.SIGUSR1)
        with fail_after(1):
            assert await outer.__anext__() == signal.SIGUSR1
