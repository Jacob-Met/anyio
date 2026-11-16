from __future__ import annotations

import types
from abc import ABCMeta, abstractmethod
from collections.abc import AsyncGenerator, Callable, Coroutine, Iterable
from typing import Any, Self, TypeVar

_T = TypeVar("_T")


class TestRunner(metaclass=ABCMeta):
    """
    Encapsulates a running event loop. Every call made through this object will use the
    same event loop.
    """

    def __init__(self) -> None:
        self._failed_fixtures: dict[object, BaseException] = {}

    def _mark_failed_fixture(
        self, fixture_id: object | None, error: BaseException
    ) -> None:
        if fixture_id is not None:
            self._failed_fixtures.setdefault(fixture_id, error)

    def _clear_failed_fixture(self, fixture_id: object | None) -> None:
        if fixture_id is not None:
            self._failed_fixtures.pop(fixture_id, None)

    def failed_fixture(
        self, fixture_ids: Iterable[object]
    ) -> tuple[object, BaseException] | None:
        for fixture_id in fixture_ids:
            if error := self._failed_fixtures.get(fixture_id):
                return fixture_id, error

        return None

    def __enter__(self) -> Self:
        return self

    @abstractmethod
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> bool | None: ...

    @abstractmethod
    def run_asyncgen_fixture(
        self,
        fixture_func: Callable[..., AsyncGenerator[_T, Any]],
        kwargs: dict[str, Any],
        fixture_id: object | None = None,
    ) -> Iterable[_T]:
        """
        Run an async generator fixture.

        :param fixture_func: the fixture function
        :param kwargs: keyword arguments to call the fixture function with
        :param fixture_id: identity used to track a failed persistent fixture
        :return: an iterator yielding the value yielded from the async generator
        """

    @abstractmethod
    def run_fixture(
        self,
        fixture_func: Callable[..., Coroutine[Any, Any, _T]],
        kwargs: dict[str, Any],
    ) -> _T:
        """
        Run an async fixture.

        :param fixture_func: the fixture function
        :param kwargs: keyword arguments to call the fixture function with
        :return: the return value of the fixture function
        """

    @abstractmethod
    def run_test(
        self, test_func: Callable[..., Coroutine[Any, Any, Any]], kwargs: dict[str, Any]
    ) -> None:
        """
        Run an async test function.

        :param test_func: the test function
        :param kwargs: keyword arguments to call the test function with
        """

    @abstractmethod
    def is_running(self) -> bool:
        """
        Check if the test runner is running.

        :return: ``True`` if the coroutine is currently being run, ``False`` otherwise.
        """
