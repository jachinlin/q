"""Dependency-free OS process memory counters for performance evidence."""

from __future__ import annotations

import os
import sys


def process_peak_rss_bytes() -> int:
    """Return the operating system's peak resident set size for this process."""
    if os.name == "nt":
        return _windows_peak_working_set_bytes()

    import resource

    peak = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    if peak <= 0:
        raise RuntimeError("operating system returned a nonpositive process peak RSS")
    return peak if sys.platform == "darwin" else peak * 1024


def _windows_peak_working_set_bytes() -> int:
    import ctypes
    from ctypes import wintypes

    class ProcessMemoryCounters(ctypes.Structure):
        _fields_ = [
            ("cb", wintypes.DWORD),
            ("page_fault_count", wintypes.DWORD),
            ("peak_working_set_size", ctypes.c_size_t),
            ("working_set_size", ctypes.c_size_t),
            ("quota_peak_paged_pool_usage", ctypes.c_size_t),
            ("quota_paged_pool_usage", ctypes.c_size_t),
            ("quota_peak_nonpaged_pool_usage", ctypes.c_size_t),
            ("quota_nonpaged_pool_usage", ctypes.c_size_t),
            ("pagefile_usage", ctypes.c_size_t),
            ("peak_pagefile_usage", ctypes.c_size_t),
        ]

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    get_current_process = kernel32.GetCurrentProcess
    get_current_process.argtypes = []
    get_current_process.restype = wintypes.HANDLE
    get_process_memory_info = ctypes.WinDLL(
        "psapi", use_last_error=True
    ).GetProcessMemoryInfo
    get_process_memory_info.argtypes = [
        wintypes.HANDLE,
        ctypes.POINTER(ProcessMemoryCounters),
        wintypes.DWORD,
    ]
    get_process_memory_info.restype = wintypes.BOOL

    counters = ProcessMemoryCounters()
    counters.cb = ctypes.sizeof(counters)
    if not get_process_memory_info(
        get_current_process(), ctypes.byref(counters), counters.cb
    ):
        raise ctypes.WinError(ctypes.get_last_error())
    peak = int(counters.peak_working_set_size)
    if peak <= 0:
        raise RuntimeError("Windows returned a nonpositive process peak RSS")
    return peak
