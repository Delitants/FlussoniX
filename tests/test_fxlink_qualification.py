"""Control-plane regressions; no daemon, FFmpeg or performance experiment."""
import asyncio
import copy
import importlib.util
from pathlib import Path
import unittest

MODULE = Path(__file__).resolve().parents[1] / 'scripts' / 'fxlink_qualification.py'
spec = importlib.util.spec_from_file_location('fxlink_qualification', MODULE)
safety = importlib.util.module_from_spec(spec)
spec.loader.exec_module(safety)


class ClosingObserver:
    """Minimal owned lifecycle used to place errors precisely at the boundary."""
    def __init__(self, *, late_error=None, close_error=None):
        self.state = {'records': 12, 'errors': []}
        self.late_error = late_error
        self.close_error = close_error
        self.closed = False

    async def close(self):
        self.closed = True
        if self.late_error:
            self.state['errors'].append(self.late_error)
        if self.close_error:
            raise self.close_error


class ObserverControls(unittest.IsolatedAsyncioTestCase):
    async def test_removed_observer_is_retained_as_terminal_evidence(self):
        book = safety.ObserverLedger()
        observer = book.register('receiver-high', ClosingObserver())
        await book.close('receiver-high')
        self.assertNotIn('receiver-high', book.active)
        self.assertEqual(book.require_complete({'receiver-high'})['receiver-high']['records'], 12)
        observer.state['records'] = 99
        self.assertEqual(book.snapshot()['receiver-high']['records'], 12)

    async def test_error_after_last_sample_prevents_success(self):
        book = safety.ObserverLedger()
        book.register('receiver-high', ClosingObserver(late_error={'kind': 'media', 'detail': 'late body mismatch'}))
        self.assertEqual(book.snapshot()['receiver-high']['errors'], [])
        with self.assertRaises(safety.MediaFailure):
            await book.close_all()
        self.assertIn('late body mismatch', str(book.snapshot()))

    async def test_close_error_does_not_skip_other_observers(self):
        book = safety.ObserverLedger()
        a = book.register('receiver-high', ClosingObserver(close_error=OSError('disk gone')))
        b = book.register('receiver-low', ClosingObserver())
        with self.assertRaises(safety.InstrumentFailure):
            await book.close_all()
        self.assertTrue(a.closed and b.closed)
        self.assertEqual(set(book.terminal), {'receiver-high', 'receiver-low'})
        self.assertIn('disk gone', str(book.snapshot()))
        self.assertFalse(book.terminal['receiver-high'].get('closed', False))
        self.assertFalse(book.terminal['receiver-high']['close_completed'])
        self.assertTrue(book.terminal['receiver-low']['close_completed'])

    async def test_missing_required_observer_prevents_success(self):
        book = safety.ObserverLedger()
        book.register('receiver-low', ClosingObserver())
        await book.close_all()
        with self.assertRaises(safety.InstrumentFailure):
            book.require_complete({'receiver-low', 'receiver-high'})

    async def test_active_observer_cannot_be_called_complete(self):
        book = safety.ObserverLedger()
        book.register('receiver-low', ClosingObserver())
        with self.assertRaises(safety.InstrumentFailure):
            book.require_complete()
        await book.close_all()

    async def test_duplicate_identity_is_rejected(self):
        book = safety.ObserverLedger()
        book.register('receiver-low', ClosingObserver())
        with self.assertRaises(safety.InstrumentFailure):
            book.register('receiver-low', ClosingObserver())
        await book.close_all()


    async def test_instrument_failure_wins_and_all_states_are_retained(self):
        book = safety.ObserverLedger()
        book.register('receiver-high', ClosingObserver(late_error={'kind': 'media', 'detail': 'corrupt frame'}))
        book.register('source-low', ClosingObserver(late_error={'kind': 'instrument', 'detail': 'capture disk failed'}))
        with self.assertRaises(safety.InstrumentFailure):
            await book.close_all()
        self.assertEqual(set(book.snapshot()), {'receiver-high', 'source-low'})

    async def test_timeout_still_closes_other_observers(self):
        class Hanging(ClosingObserver):
            async def close(self):
                await asyncio.Event().wait()
        book = safety.ObserverLedger(close_timeout=.01)
        book.register('stuck', Hanging())
        good = book.register('receiver-low', ClosingObserver())
        with self.assertRaises(safety.InstrumentFailure):
            await book.close_all()
        self.assertTrue(good.closed)
        self.assertEqual(set(book.terminal), {'stuck', 'receiver-low'})
        self.assertIn('TimeoutError', str(book.snapshot()))

    async def test_cancellation_waits_for_terminal_capture_and_closes_siblings(self):
        entered, release = asyncio.Event(), asyncio.Event()
        class Controlled(ClosingObserver):
            async def close(self):
                entered.set()
                await release.wait()
                self.closed = True
        book = safety.ObserverLedger()
        first = book.register('receiver-high', Controlled())
        second = book.register('receiver-low', ClosingObserver())
        task = asyncio.create_task(book.close_all())
        await entered.wait()
        task.cancel()
        await asyncio.sleep(0)
        release.set()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(first.closed and second.closed)
        self.assertEqual(set(book.terminal), {'receiver-high', 'receiver-low'})

    async def test_close_is_idempotent_and_returns_defensive_copy(self):
        book = safety.ObserverLedger()
        book.register('receiver-low', ClosingObserver())
        a = await book.close('receiver-low')
        a['errors'].append('caller mutation')
        b = await book.close('receiver-low')
        self.assertEqual(b['errors'], [])

    async def test_untyped_legacy_error_is_conservatively_instrument_failure(self):
        book = safety.ObserverLedger()
        book.register('receiver-low', ClosingObserver(late_error='unclassified read error'))
        with self.assertRaises(safety.InstrumentFailure):
            await book.close_all()


class NativeControls(unittest.TestCase):
    def row(self, t, missed=0, bootstrap=0):
        return dict(wall_ms=t, missed_records=missed, bootstrap_records=bootstrap)

    def test_invalid_middle_cannot_hide_between_valid_endpoints(self):
        rows = [self.row(1), self.row(2, missed=1), self.row(3)]
        with self.assertRaises(safety.MediaFailure):
            safety.native_window(rows, 1, 3)

    def test_missed_bootstrap_snapshot_prevents_qualification(self):
        rows = [self.row(0, missed=1, bootstrap=2), self.row(1), self.row(3)]
        with self.assertRaises(safety.MediaFailure):
            safety.native_window(rows, 1, 3)

    def test_missing_counter_is_instrument_failure(self):
        with self.assertRaises(safety.InstrumentFailure):
            safety.native_window([dict(wall_ms=1, bootstrap_records=0)], 1, 3)

    def test_good_endpoints_are_selected_without_mutating_evidence(self):
        rows = [self.row(0, bootstrap=2), self.row(1), self.row(3), self.row(4)]
        before = copy.deepcopy(rows)
        self.assertEqual(safety.native_window(rows, 1, 3), rows[1:3])
        self.assertEqual(rows, before)


class ClassificationControls(unittest.TestCase):
    def test_typed_instrument_failure_aborts_every_media_phase(self):
        for phase in safety.MEDIA_PHASES:
            with self.subTest(phase=phase):
                self.assertEqual(safety.failure_class(safety.InstrumentFailure('observer setup'), phase), 'instrument')

    def test_unexpected_exception_is_never_called_media_failure(self):
        self.assertEqual(safety.failure_class(KeyError('broken evidence'), 'both'), 'instrument')

    def test_media_failure_remains_separate(self):
        self.assertEqual(safety.failure_class(safety.MediaFailure('native loss'), 'both'), 'media_or_acceptance')
        self.assertEqual(safety.failure_class(AssertionError('restart'), 'both'), 'media_or_acceptance')


if __name__ == '__main__':
    unittest.main()
