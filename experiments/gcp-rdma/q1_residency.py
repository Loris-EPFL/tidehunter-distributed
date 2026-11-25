"""Probe or evict exact file ranges without reading or faulting their pages."""

import ctypes
import mmap
import os
import stat
from pathlib import Path

from q1_manifest import integer
from q1_package import relative_path, require

CHUNK = 256 << 20


def measure(root: Path, name, offset, length, *, evict=False):
    path = root / relative_path(name)
    require(root.is_absolute() and root.resolve() == root and path.resolve() == path,
            "residency file outside canonical run root")
    page = mmap.PAGESIZE
    require(integer(offset, 0) and offset % page == 0 and integer(length), "invalid residency range")
    libc = ctypes.CDLL(None, use_errno=True)
    mincore = libc.mincore
    mincore.argtypes = (ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p)
    mincore.restype = ctypes.c_int
    fd = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        st = os.fstat(fd)
        require(stat.S_ISREG(st.st_mode) and offset + length <= st.st_size
                and (length % page == 0 or offset + length == st.st_size),
                "residency range is not aligned or exceeds the file")
        if evict:
            os.posix_fadvise(fd, offset, length, os.POSIX_FADV_DONTNEED)
        selected = 0
        resident = 0
        for begin in range(offset, offset + length, CHUNK):
            size = min(CHUNK, offset + length - begin)
            pages = (size + page - 1) // page
            result = (ctypes.c_ubyte * pages)()
            # Private writable mapping permits getting its address without writing to it.
            with mmap.mmap(fd, size, access=mmap.ACCESS_COPY, offset=begin) as region:
                address = ctypes.c_char.from_buffer(region)
                try:
                    if mincore(ctypes.addressof(address), size, result):
                        error = ctypes.get_errno()
                        raise OSError(error, os.strerror(error), path)
                finally:
                    del address
            selected += pages
            resident += sum(r & 1 for r in result)
        return {"selected": selected, "resident": resident}
    finally:
        os.close(fd)
