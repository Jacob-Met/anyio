from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import AbstractContextManager
from signal import Signals

from ._eventloop import get_async_backend


def open_signal_receiver(
    *signals: Signals,
) -> AbstractContextManager[AsyncIterator[Signals]]:
    """
    Start receiving operating system signals.

    :param signals: signals to receive (e.g. ``signal.SIGINT``)
    :return: an asynchronous context manager for an asynchronous iterator which yields
        signal numbers
    :raises NoEventLoopError: if no supported asynchronous event loop is running in the
        current thread

    .. warning:: Windows does not support signals natively so it is best to avoid
        relying on this in cross-platform applications.

    .. note:: On asyncio, signal handlers installed using
        :meth:`~asyncio.loop.add_signal_handler` continue receiving signals while
        the receiver is open. They remain registered when the receiver closes.

    """
    return get_async_backend().open_signal_receiver(*signals)
