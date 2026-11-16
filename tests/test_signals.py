from __future__ import annotations

import asyncio
import os
import signal
import sys
from collections.abc import AsyncIterable
from types import FrameType

import pytest

from anyio import create_task_group, fail_after, open_signal_receiver, to_thread

from .conftest import asyncio_params

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


async def test_restore_python_signal_handler() -> None:
    # Regression test for #689: a user-installed handler should work again.
    received: list[int] = []

    def previous_handler(signum: int, frame: FrameType | None) -> None:
        received.append(signum)

    old_handler = signal.getsignal(signal.SIGUSR1)
    signal.signal(signal.SIGUSR1, previous_handler)
    try:
        with open_signal_receiver(signal.SIGUSR1) as sigiter:
            await to_thread.run_sync(os.kill, os.getpid(), signal.SIGUSR1)
            with fail_after(1):
                assert await anext(sigiter) == signal.SIGUSR1

        assert signal.getsignal(signal.SIGUSR1) is previous_handler
        await to_thread.run_sync(os.kill, os.getpid(), signal.SIGUSR1)
        assert received == [signal.SIGUSR1]
    finally:
        signal.signal(signal.SIGUSR1, old_handler)


@pytest.mark.parametrize("anyio_backend", asyncio_params)
async def test_restore_asyncio_signal_handler() -> None:
    # Existing event-loop callbacks must survive signal receiver replacement.
    loop = asyncio.get_running_loop()
    received = asyncio.Event()
    loop.add_signal_handler(signal.SIGUSR1, received.set)
    try:
        with open_signal_receiver(signal.SIGUSR1) as sigiter:
            await to_thread.run_sync(os.kill, os.getpid(), signal.SIGUSR1)
            with fail_after(1):
                assert await anext(sigiter) == signal.SIGUSR1

        received.clear()
        await to_thread.run_sync(os.kill, os.getpid(), signal.SIGUSR1)
        with fail_after(1):
            await received.wait()
    finally:
        loop.remove_signal_handler(signal.SIGUSR1)


async def test_nested_signal_receivers() -> None:
    # Closing an inner receiver must restore the active outer receiver.
    with open_signal_receiver(signal.SIGUSR1) as outer:
        with open_signal_receiver(signal.SIGUSR1) as inner:
            await to_thread.run_sync(os.kill, os.getpid(), signal.SIGUSR1)
            with fail_after(1):
                assert await anext(inner) == signal.SIGUSR1

        await to_thread.run_sync(os.kill, os.getpid(), signal.SIGUSR1)
        with fail_after(1):
            assert await anext(outer) == signal.SIGUSR1


@pytest.mark.parametrize("anyio_backend", asyncio_params)
async def test_restore_signal_handler_when_registration_fails() -> None:
    previous = signal.getsignal(signal.SIGUSR1)

    def handler(signum: int, frame: FrameType | None) -> None:
        pass

    signal.signal(signal.SIGUSR1, handler)
    try:
        with pytest.raises(OSError):
            with open_signal_receiver(signal.SIGUSR1, signal.SIGKILL):
                pytest.fail("SIGKILL must not be registered")

        assert signal.getsignal(signal.SIGUSR1) is handler
    finally:
        signal.signal(signal.SIGUSR1, previous)
