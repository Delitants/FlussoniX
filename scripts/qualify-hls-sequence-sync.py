"""Check the first HLS sequence record's directory fsync order on Linux.

Pass a freshly built FlussoniX library test executable. This checks the
syscalls that protect the first sequence record; it is not a power-cut test.
"""

import argparse
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import tempfile


FIXTURE = "hls_generation::tests::fresh_media_root_can_create_its_first_private_floor"


def trace(binary: Path, output: Path) -> list[str]:
    command = [
        "strace", "-f", "-e", "trace=openat,mkdir,fsync,rename", "-o", str(output),
        str(binary), "--exact", FIXTURE,
    ]
    process = subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        start_new_session=True,
    )
    try:
        stdout, stderr = process.communicate(timeout=20)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.communicate()
        raise RuntimeError("HLS sequence fixture exceeded 20 seconds") from None
    if process.returncode:
        raise RuntimeError(f"HLS sequence fixture failed: {stdout[-800:]} {stderr[-800:]}")
    return output.read_text().splitlines()


def check(lines: list[str]) -> None:
    root = None
    opened: dict[tuple[str, str], Path] = {}
    synced: set[tuple[int, Path]] = set()
    record_rename = None
    temporary_record = None
    for index, line in enumerate(lines):
        created = re.match(r'^(\d+) mkdir\("([^"]+/new-media-root)",[^)]*\) = 0$', line)
        if created:
            root = Path(created.group(2))
        opened_fd = re.match(r'^(\d+) openat\(AT_FDCWD, "([^"]+)",.*\) = (\d+)$', line)
        if opened_fd:
            opened[(opened_fd.group(1), opened_fd.group(3))] = Path(opened_fd.group(2))
        flushed = re.match(r'^(\d+) fsync\((\d+)\)\s+= 0$', line)
        if flushed and (flushed.group(1), flushed.group(2)) in opened:
            synced.add((index, opened[(flushed.group(1), flushed.group(2))]))
        renamed = re.match(r'^(\d+) rename\("([^"]+)", "([^"]+)"\) = 0$', line)
        if renamed and root and Path(renamed.group(2)).parent == root / ".hls-sequences":
            temporary_record = Path(renamed.group(2))
            record_rename = index
    if root is None or record_rename is None or temporary_record is None:
        raise RuntimeError("fixture did not create and rename its HLS sequence record")
    before_publication = {path for index, path in synced if index < record_rename}
    missing = [
        str(path)
        for path in (root.parent.parent, root.parent, root, temporary_record)
        if path not in before_publication
    ]
    if missing:
        raise RuntimeError("missing fsync before record rename: " + ", ".join(missing))
    if not any(index > record_rename and path == root / ".hls-sequences" for index, path in synced):
        raise RuntimeError("sequence directory was not synced after record rename")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--test-binary", required=True, type=Path)
    args = parser.parse_args()
    if shutil.which("strace") is None:
        parser.error("strace is required")
    if not args.test_binary.is_file():
        parser.error("test binary is missing")
    with tempfile.TemporaryDirectory(prefix="flussonix-hls-sync-trace-") as temporary:
        check(trace(args.test_binary, Path(temporary) / "syscalls"))
    print("HLS sequence parent directories synced before record publication")


if __name__ == "__main__":
    main()
