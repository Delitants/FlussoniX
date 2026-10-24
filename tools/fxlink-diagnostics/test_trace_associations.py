"""Owned binary inputs and independent correlation invariants; no timing oracle."""
import copy
import hashlib
import os
import random
import tempfile
import unittest
from pathlib import Path
from packet_trace import HEADER, EVENT, read_trace, correlate

CONN = b"c" * 16
PAYLOAD = hashlib.sha256(b"owned-source-copy").digest()
EMPTY = hashlib.sha256(b"").digest()

def event(stage, numbers=(0, 0, 0), key=EMPTY, length=0, conn=CONN):
    return (stage, length, conn, key, numbers)

def accepted(events, label):
    """Round-trip real bounded FXTRACE1 bytes before any counting wrapper."""
    root = os.environ.get("FLUSSONIX_TRACE_ASSOCIATION_ARTIFACT_DIR")
    temporary = None
    if root:
        directory = Path(root)
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    else:
        temporary = tempfile.TemporaryDirectory()
        directory = Path(temporary.name)
    path = directory / (label + ".trace")
    count = len(events)
    assert 1 <= count <= 131072
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(fd, "wb") as output:
        output.write(HEADER.pack(b"FXTRACE1", 96, 1, count, count, 0, 0, 1, count))
        for seq, (stage, length, conn, key, numbers) in enumerate(events, 1):
            output.write(EVENT.pack(seq, seq, stage, 0, length, conn, key, *numbers))
    result = read_trace(path)
    assert result["complete"] and len(result["events"]) == count
    assert path.stat().st_size == 64 + 96 * count
    assert path.stat().st_mode & 0o777 == 0o600
    if temporary:
        temporary.cleanup()
    return result

class CountedNumbers(list):
    def __init__(self, values, counter, kind):
        super().__init__(values)
        self.counter, self.kind = counter, kind
    def __getitem__(self, key):
        packet_key = isinstance(key, slice) and key.start in (None, 0) and key.stop == 2 and key.step is None
        if (self.kind == "packet" and packet_key) or (self.kind == "segment" and key == 0):
            self.counter[0] += 1
        return super().__getitem__(key)

def wrap_numbers(trace, stage, kind):
    count = [0]
    for item in trace["events"]:
        if item["stage"] == stage:
            item["numbers"] = CountedNumbers(item["numbers"], count, kind)
    return count

def duplicate_packets(n):
    return ([event(1, key=PAYLOAD, length=1)] * n +
            [event(2)] +
            [event(3, (7, 2, 0), PAYLOAD, 1)] * n +
            [event(4, (7, 2, 0), hashlib.sha256(b"cipher").digest(), 8)] * n +
            [event(6)])

def shared_packet_segments(n):
    keys = [hashlib.sha256(i.to_bytes(8, "big")).digest() for i in range(n)]
    return ([event(1, key=PAYLOAD, length=1)] * n +
            [event(2)] +
            [event(3, (7, 2, 0), PAYLOAD, 1)] * n +
            [event(4, (7, 2, 8 * (n - 1)), keys[-1], 8)] +
            [event(5, (8 * i, 0, 0), key, 8) for i, key in enumerate(keys)] +
            [event(6), event(9, key=keys[-1], length=8)]), keys[-1].hex()

class ComplexityTests(unittest.TestCase):
    def packet_case(self, n):
        trace = accepted(duplicate_packets(n), "packet-" + str(n))
        count = wrap_numbers(trace, 4, "packet")
        rows = correlate([trace])
        self.assertEqual(len(rows), n)
        self.assertEqual([r["copy"] for r in rows], list(range(1, n + 1)))
        self.assertTrue(all(r["packet"] == [7, 2] and r["cipher"] is None for r in rows))
        self.assertTrue(all(r["boundary"] == "frame observed; encrypted UDP mapping unavailable" for r in rows))
        print(f"packet_n={n},packet_key_reads={count[0]},bound={4*n},output_exact=true", flush=True)
        self.assertLessEqual(count[0], 4 * n, "one packet-list scan per frame is prohibited")
    def segment_case(self, n):
        events, expected = shared_packet_segments(n)
        trace = accepted(events, "segment-" + str(n))
        count = wrap_numbers(trace, 5, "segment")
        rows = correlate([trace])
        self.assertEqual(len(rows), n)
        self.assertTrue(all(r["packet"] == [7, 2] and r["cipher"] == expected for r in rows))
        self.assertTrue(all(r["boundary"] == "unresolved after UDP acceptance" for r in rows))
        print(f"segment_n={n},segment_start_reads={count[0]},bound={5*n},output_exact=true", flush=True)
        self.assertLessEqual(count[0], 5 * n, "one segment-list scan per queued frame is prohibited")
    def test_packet_lookup_has_bounded_work(self):
        self.packet_case(1024)
    def test_interval_lookup_has_bounded_work(self):
        self.segment_case(1024)

class FileLimitTests(ComplexityTests):
    # Avoid inheriting the two RED-scale methods as duplicate cases.
    test_packet_lookup_has_bounded_work = None
    test_interval_lookup_has_bounded_work = None
    def test_near_limit_packet_fixture(self):
        self.packet_case(40000)
    def test_near_limit_interval_fixture(self):
        self.segment_case(40000)

def batch(packets, segments, frames=None, queued=1, tail=()):
    frames = frames if frames is not None else [packets[0][0]]
    records = [event(1, key=PAYLOAD, length=1)] * queued + [event(2)]
    records += [event(3, (pn, space, 123), PAYLOAD, 1) for pn, space in frames]
    records += [event(4, (pn, space, start), hashlib.sha256(b"packet").digest(), length)
                for (pn, space), start, length in packets]
    records += [event(5, (start, 999, 123), hashlib.sha256(key).digest(), length)
                for start, length, key in segments]
    return records + [event(6)] + list(tail)

class SemanticsTests(unittest.TestCase):
    def test_inclusive_intervals_duplicate_records_and_unbounded_python_endpoints(self):
        cases = [
            ([(0, 8, b"a")], 0, 8, b"a"),
            ([(0, 8, b"a"), (8, 8, b"b")], 8, 0, None),
            ([(8, 0, b"a")], 8, 0, b"a"),
            ([(8, 0, b"a"), (8, 0, b"a")], 8, 0, None),
            ([(0, 20, b"a"), (5, 10, b"b")], 6, 1, None),
            ([(0, 20, b"a"), (5, 2, b"b")], 9, 4, b"a"),
            ([(5, 2, b"b"), (0, 20, b"a")], 9, 4, b"a"),
            ([(0, 7, b"a")], 7, 1, None),
            ([(2**64 - 1, 2**32 - 1, b"a")], 2**64 - 1, 2**32 - 1, b"a"),
        ]
        for i, (segments, start, length, key) in enumerate(cases):
            with self.subTest(i=i):
                trace = accepted(batch([((7, 2), start, length)], segments), "interval-" + str(i))
                original = copy.deepcopy(trace)
                row = correlate([trace])[0]
                self.assertEqual(row["cipher"], None if key is None else hashlib.sha256(key).hexdigest())
                self.assertEqual(row["packet"], [7, 2])
                self.assertEqual(trace, original, "correlation must not mutate input events")
    def test_duplicate_packet_records_consume_fifo_copy_but_remain_ambiguous(self):
        packets = [((7, 2), 0, 8), ((7, 2), 0, 8), ((8, 2), 8, 8)]
        trace = accepted(batch(packets, [(0, 8, b"a"), (8, 8, b"b")], [(7, 2), (8, 2)], queued=2), "duplicate-fifo")
        rows = correlate([trace])
        self.assertEqual([r["packet"] for r in rows], [[7, 2], [8, 2]])
        self.assertEqual([r["cipher"] for r in rows], [None, hashlib.sha256(b"b").hexdigest()])
        self.assertEqual([r["copy"] for r in rows], [1, 2])
    def test_packet_number_space_and_extreme_numeric_keys_are_distinct(self):
        keys = [(0, 2), (2**61 - 1, 2), (2**64 - 1, 2**64 - 1), (7, 1), (7, 2)]
        packets = [(key, i * 8, 8) for i, key in enumerate(keys)]
        segments = [(i * 8, 8, bytes([i])) for i in range(len(keys))]
        rows = correlate([accepted(batch(packets[::-1], segments[::-1], keys, len(keys)), "numeric-keys")])
        self.assertEqual([r["packet"] for r in rows], [list(key) for key in keys])
        self.assertEqual([r["cipher"] for r in rows], [hashlib.sha256(bytes([i])).hexdigest() for i in range(len(keys))])
    def test_missing_packet_consumes_copy_and_frames_may_share_one_packet(self):
        trace = accepted(batch([((7, 2), 0, 8)], [(0, 8, b"a")], [(9, 2), (7, 2), (7, 2)], queued=3), "shared-packet")
        rows = correlate([trace])
        self.assertEqual([r["packet"] for r in rows], [[9, 2], [7, 2], [7, 2]])
        self.assertEqual([r["cipher"] for r in rows], [None] + [hashlib.sha256(b"a").hexdigest()] * 2)
    def test_fifo_deferred_queue_and_batch_reset_are_preserved(self):
        records = [event(2), event(3, (99, 2, 0), PAYLOAD, 1), event(1, key=PAYLOAD, length=1),
                   event(2), event(3, (7, 2, 0), PAYLOAD, 1), event(4, (7, 2, 0), length=8),
                   event(5, key=hashlib.sha256(b"a").digest(), length=8), event(6),
                   event(1, key=PAYLOAD, length=1), event(2), event(3, (8, 2, 0), PAYLOAD, 1)]
        rows = correlate([accepted(records, "batch-reset")])
        self.assertEqual([r["packet"] for r in rows], [[7, 2], None])
        self.assertEqual([r["copy"] for r in rows], [1, 2])
    def test_node_and_connection_isolation_with_global_stage_evidence(self):
        cipher = hashlib.sha256(b"a").digest()
        first = accepted(batch([((7, 2), 0, 8)], [(0, 8, b"a")]), "node-first")
        second = accepted([event(1, key=PAYLOAD, length=1), event(9, key=cipher, length=8)], "node-second")
        third_records = batch([((7, 2), 0, 8)], [(0, 8, b"a")])
        third_records[0] = event(1, key=PAYLOAD, length=1, conn=b"d" * 16)
        third = accepted(third_records, "conn-mismatch")
        rows = correlate([first, second, third])
        self.assertEqual([r["node"] for r in rows], [0, 1, 2])
        self.assertEqual([r["cipher"] for r in rows], [cipher.hex(), None, None])
        self.assertEqual(rows[0]["boundary"], "unresolved after UDP acceptance")
    def test_incomplete_and_source_ambiguous_rows_still_keep_associations(self):
        for i, (complete, evicting) in enumerate([(False, False), (True, True)]):
            records = batch([((7, 2), 0, 8)], [(0, 8, b"a")])
            if evicting:
                records[0] = event(1, (1, 0, 0), PAYLOAD, 1)
            trace = accepted(records, "incomplete-" + str(i))
            trace["complete"] = complete
            row = correlate([trace])[0]
            self.assertEqual(row["cipher"], hashlib.sha256(b"a").hexdigest())
            self.assertEqual(row["boundary"], "incomplete trace; no absence inference" if not complete else "source-copy identity ambiguous")
    def test_small_random_intervals_match_independent_bruteforce_uniqueness(self):
        rng = random.Random(20261023)
        for case in range(80):
            segments = [(rng.randrange(20), rng.randrange(12), bytes([i])) for i in range(rng.randrange(20))]
            packets = [((i, i % 3), rng.randrange(25), rng.randrange(10)) for i in range(20)]
            rng.shuffle(segments)
            rows = correlate([accepted(batch(packets, segments, [p[0] for p in packets], len(packets)), "random-" + str(case))])
            for row, (_, start, length) in zip(rows, packets):
                matches = [key for at, size, key in segments if at <= start and start + length <= at + size]
                expected = hashlib.sha256(matches[0]).hexdigest() if len(matches) == 1 else None
                self.assertEqual(row["cipher"], expected)
if __name__ == "__main__":
    unittest.main()
