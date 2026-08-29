"""验证单用户数据流水线的数据根独占执行锁。"""

from __future__ import annotations

import multiprocessing
import os
import threading
from pathlib import Path
from queue import Queue
from typing import Protocol

import pytest

from quant_research.data.storage.paths import DataRootExecutionLock
from quant_research.domain.errors import QuantError


class _ProcessEvent(Protocol):
    """描述多进程测试所需的最小事件接口。"""

    def set(self) -> None:
        """将事件标记为已触发。"""

    def wait(self, timeout: float | None = None) -> bool:
        """等待事件触发并返回是否成功。"""


class _LockTestSupport:
    """集中承载可由 spawn 进程调用的锁测试入口。"""

    @staticmethod
    def hold_until_released(
        data_root: str,
        acquired: _ProcessEvent,
        release: _ProcessEvent,
    ) -> None:
        """持有数据根锁，直到父进程允许释放。"""
        with DataRootExecutionLock(Path(data_root)):
            acquired.set()
            assert release.wait(timeout=10)

    @staticmethod
    def acquire_then_crash(data_root: str, acquired: _ProcessEvent) -> None:
        """取得数据根锁后无清理退出，用于验证 OS 自动释放。"""
        lock = DataRootExecutionLock(Path(data_root))
        lock.__enter__()
        acquired.set()
        os._exit(0)


def test_data_root_lock_is_reentrant_in_the_owning_thread(tmp_path: Path) -> None:
    """同一线程的组合流水线可以安全嵌套各阶段入口。"""
    first = DataRootExecutionLock(tmp_path)
    second = DataRootExecutionLock(tmp_path)

    with first, second:
        assert (tmp_path / "state" / "data-pipeline.lock").is_file()


def test_data_root_lock_fails_fast_for_another_thread(tmp_path: Path) -> None:
    """同一进程的其他线程不会等待或进入同一数据根。"""
    result: Queue[str] = Queue()

    def contend() -> None:
        try:
            with DataRootExecutionLock(tmp_path):
                result.put("acquired")
        except QuantError as error:
            result.put(error.detail.code)

    with DataRootExecutionLock(tmp_path):
        contender = threading.Thread(target=contend)
        contender.start()
        contender.join(timeout=5)

    assert not contender.is_alive()
    assert result.get_nowait() == "DATA_PIPELINE_ALREADY_RUNNING"


def test_data_root_lock_fails_fast_for_another_process(tmp_path: Path) -> None:
    """不同进程争用相同数据根时返回稳定结构化错误。"""
    context = multiprocessing.get_context("spawn")
    acquired = context.Event()
    release = context.Event()
    holder = context.Process(
        target=_LockTestSupport.hold_until_released,
        args=(str(tmp_path), acquired, release),
    )
    holder.start()
    assert acquired.wait(timeout=10)

    try:
        with (
            pytest.raises(QuantError) as error,
            DataRootExecutionLock(tmp_path),
        ):
            pytest.fail("contender acquired a data root already owned by a process")
        assert error.value.detail.code == "DATA_PIPELINE_ALREADY_RUNNING"
        assert error.value.detail.retryable is True
    finally:
        release.set()
        holder.join(timeout=10)

    assert holder.exitcode == 0


def test_data_root_lock_is_released_when_the_process_crashes(tmp_path: Path) -> None:
    """持有者崩溃后无需 PID 探测或陈旧锁回收即可再次取得锁。"""
    context = multiprocessing.get_context("spawn")
    acquired = context.Event()
    holder = context.Process(
        target=_LockTestSupport.acquire_then_crash,
        args=(str(tmp_path), acquired),
    )
    holder.start()
    assert acquired.wait(timeout=10)
    holder.join(timeout=10)
    assert holder.exitcode == 0

    with DataRootExecutionLock(tmp_path):
        assert (tmp_path / "state" / "data-pipeline.lock").is_file()


def test_data_root_lock_does_not_serialize_different_roots(tmp_path: Path) -> None:
    """相互独立的数据根可以由同一线程同时持有。"""
    first_root = tmp_path / "first"
    second_root = tmp_path / "second"

    with DataRootExecutionLock(first_root), DataRootExecutionLock(second_root):
        assert (first_root / "state" / "data-pipeline.lock").is_file()
        assert (second_root / "state" / "data-pipeline.lock").is_file()
