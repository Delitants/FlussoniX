#!/usr/bin/env python3
"""Assess one completed isolated HTTP MPEG-TS soak report."""

import argparse
import json
import math
import os
from pathlib import Path
import re
import stat


DAY = 86_400
MIB = 1_048_576
GIB = 1_073_741_824
MAX_REPORT_BYTES = 8 * MIB
EXPECTED_VIEWERS = {f"{stream}:{index}" for stream in ("soak_a", "soak_b") for index in range(4)}


def number(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    try:
        return math.isfinite(value)
    except OverflowError:
        return False


def reject_nonfinite(_):
    raise ValueError("non-finite JSON number")


def read_report(path):
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags)
    try:
        info = os.fstat(fd)
        if (
            not stat.S_ISREG(info.st_mode)
            or (info.st_uid != os.geteuid() and os.geteuid() != 0)
            or info.st_mode & 0o077
            or info.st_size > MAX_REPORT_BYTES
        ):
            raise ValueError("private regular report required")
        with os.fdopen(fd, "rb", closefd=False) as stream:
            data = stream.read(MAX_REPORT_BYTES + 1)
        if len(data) > MAX_REPORT_BYTES:
            raise ValueError("report too large")
        report = json.loads(data, parse_constant=reject_nonfinite)
        if not isinstance(report, dict):
            raise ValueError("report must be an object")
        return report
    finally:
        os.close(fd)


def assess(report, revision):
    issues = []
    def fail(code):
        if code not in issues:
            issues.append(code)

    if report.get("revision") != revision[:7]:
        fail("revision_mismatch")
    elapsed = report.get("elapsed_seconds")
    if (
        report.get("state") != "completed"
        or report.get("duration_seconds") != DAY
        or not number(elapsed)
        or elapsed < DAY
        or report.get("daemon_exited_early") is not False
    ):
        fail("run_incomplete")
    if report.get("streams") != 2 or report.get("viewers_per_stream") != 4:
        fail("workload_mismatch")

    samples = report.get("samples")
    if not isinstance(samples, list) or not samples or not all(isinstance(row, dict) for row in samples):
        return {"result": "fail", "issues": issues + ["invalid_report"]}, 2
    elapsed_samples = [row.get("elapsed") for row in samples]
    memories = [row.get("memory_bytes") for row in samples]
    memory_peaks = [row.get("memory_peak_bytes") for row in samples]
    media_sizes = [row.get("media_dir_bytes") for row in samples]
    oom_kills = [row.get("oom_kills") for row in samples]
    if not all(number(v) and v >= 0 for values in (
        elapsed_samples, memories, memory_peaks, media_sizes, oom_kills
    ) for v in values):
        return {"result": "fail", "issues": issues + ["invalid_report"]}, 2
    if (
        report.get("sample_seconds") != 60
        or len(samples) < 1439
        or elapsed_samples[0] > 5
        or elapsed_samples[-1] < DAY - 65
    ):
        fail("samples_incomplete")
    if any(b <= a or b - a > 90 for a, b in zip(elapsed_samples, elapsed_samples[1:])):
        fail("samples_irregular")
    if max(memories) > 512 * MIB or max(memory_peaks) > 512 * MIB:
        fail("memory_limit_exceeded")
    if memories[-1] - memories[0] > 64 * MIB:
        fail("memory_growth_exceeded")
    if max(media_sizes) > 512 * MIB:
        fail("media_disk_exceeded")
    if max(oom_kills) > 0:
        fail("oom_kill")

    viewers = report.get("viewers")
    if not isinstance(viewers, dict):
        return {"result": "fail", "issues": issues + ["invalid_report"]}, 2
    if set(viewers) != EXPECTED_VIEWERS:
        fail("viewer_set_incomplete")
    max_media_gap = 0
    max_reconnect_gap = 0
    total_bytes = 0
    for name in EXPECTED_VIEWERS & set(viewers):
        row = viewers[name]
        if not isinstance(row, dict) or not all(number(row.get(field)) and row[field] >= 0 for field in (
            "bytes", "connections", "sync_errors", "media_gap_max_ms", "reconnect_gap_max_ms"
        )):
            return {"result": "fail", "issues": issues + ["invalid_report"]}, 2
        total_bytes += row["bytes"]
        if not number(total_bytes):
            return {"result": "fail", "issues": issues + ["invalid_report"]}, 2
        max_media_gap = max(max_media_gap, row["media_gap_max_ms"])
        max_reconnect_gap = max(max_reconnect_gap, row["reconnect_gap_max_ms"])
        if row["bytes"] < 4 * GIB:
            fail("viewer_media_incomplete")
        if row["connections"] < (1440 if name.endswith(":0") else 1):
            fail("viewer_churn_incomplete")
        statuses = row.get("statuses")
        if not isinstance(statuses, dict) or statuses != {"200": row["connections"]}:
            fail("viewer_http_failure")
        if row.get("errors") != {}:
            fail("viewer_error")
        if row["sync_errors"] != 0:
            fail("ts_sync_error")
    if max_media_gap > 3000:
        fail("media_gap_exceeded")
    if max_reconnect_gap > 3000:
        fail("reconnect_gap_exceeded")

    metadata = report.get("metadata_checks")
    if not isinstance(metadata, dict) or metadata.get("success") != len(samples) or metadata.get("errors") != {}:
        fail("metadata_check_failed")
    result = {
        "result": "fail" if issues else "pass",
        "issues": issues,
        "samples": len(samples),
        "viewer_gib": round(total_bytes / GIB, 2),
        "max_media_gap_ms": round(max_media_gap, 2),
        "max_reconnect_gap_ms": round(max_reconnect_gap, 2),
        "cgroup_memory_peak_mib": round(max(memory_peaks) / MIB, 2),
        "sampled_memory_growth_mib": round((memories[-1] - memories[0]) / MIB, 2),
    }
    return result, 1 if issues else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--revision", required=True)
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9a-f]{40}", args.revision):
        parser.error("revision must be a full lowercase Git SHA-1")
    try:
        report = read_report(args.report)
    except (OSError, ValueError, UnicodeError, RecursionError):
        print(json.dumps({"result": "fail", "issues": ["invalid_report"]}))
        raise SystemExit(2)
    result, status = assess(report, args.revision)
    try:
        output = json.dumps(result, sort_keys=True, allow_nan=False)
    except (ValueError, OverflowError):
        output = json.dumps({"result": "fail", "issues": ["invalid_report"]})
        status = 2
    print(output)
    raise SystemExit(status)


if __name__ == "__main__":
    main()
