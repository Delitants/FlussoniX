"""Evidence safety for FXLink qualification; does not configure media or transport."""
import asyncio
import copy
import math
import subprocess
import urllib.error

MEDIA_PHASES = frozenset({
    'first_ready', 'warm', 'second_ready', 'both_decode', 'both',
    'acceptance', 'remove_high', 'after_delete',
})


class InstrumentFailure(RuntimeError):
    """Evidence collection failed; abort the remaining cohort."""


class MediaFailure(RuntimeError):
    """Observed media or acceptance failure; retain the failed case."""


def failure_class(error, phase):
    if isinstance(error, InstrumentFailure):
        return 'instrument'
    if isinstance(error, MediaFailure):
        return 'media_or_acceptance'
    known_media = (AssertionError, urllib.error.URLError, TimeoutError,
                   ConnectionResetError, subprocess.SubprocessError)
    if phase in MEDIA_PHASES and isinstance(error, known_media):
        return 'media_or_acceptance'
    return 'instrument'


def check_observers(states):
    failures = []
    for name, state in states.items():
        errors = state.get('errors')
        if not isinstance(errors, list):
            failures.append(InstrumentFailure(f'{name}: missing observer error evidence'))
            continue
        for error in errors:
            if isinstance(error, dict) and error.get('kind') == 'media':
                failures.append(MediaFailure(f'{name}: {error.get("detail", "media failure")}'))
            else:
                failures.append(InstrumentFailure(f'{name}: {error!r}'))
    if failures:
        raise next((e for e in failures if isinstance(e, InstrumentFailure)), failures[0])


class ObserverLedger:
    """Own each observer through close, then retain immutable terminal evidence."""
    def __init__(self, close_timeout=5):
        if not math.isfinite(close_timeout) or close_timeout <= 0:
            raise ValueError('observer close timeout must be positive and finite')
        self.active = {}
        self.terminal = {}
        self.started = set()
        self.close_timeout = close_timeout

    def register(self, name, observer):
        if name in self.started:
            raise InstrumentFailure(f'duplicate observer lifetime: {name}')
        self.started.add(name)
        self.active[name] = observer
        return observer

    def snapshot(self):
        states = copy.deepcopy(self.terminal)
        states.update({name: copy.deepcopy(obs.state) for name, obs in self.active.items()})
        return states

    async def close(self, name):
        if name in self.terminal:
            check_observers({name: self.terminal[name]})
            return copy.deepcopy(self.terminal[name])
        if name not in self.active:
            raise InstrumentFailure(f'unknown observer lifetime: {name}')
        observer = self.active[name]
        error = None
        cancelled = False
        work = asyncio.create_task(asyncio.wait_for(observer.close(), self.close_timeout))
        try:
            await asyncio.shield(work)
        except asyncio.CancelledError:
            if asyncio.current_task().cancelling():
                cancelled = True
                try:
                    await work
                except asyncio.CancelledError:
                    error = InstrumentFailure('observer close cancelled unexpectedly')
                except Exception as exc:
                    error = exc
            else:
                error = InstrumentFailure('observer close cancelled unexpectedly')
        except Exception as exc:
            error = exc
        finally:
            try:
                state = copy.deepcopy(observer.state)
                if not isinstance(state, dict) or not isinstance(state.get('errors'), list):
                    raise ValueError('invalid observer terminal state')
            except Exception as exc:
                state = {'errors': [{'kind': 'instrument', 'detail': f'terminal capture: {exc!r}'}]}
            if error:
                state['errors'].append({'kind': 'media' if isinstance(error, MediaFailure) else 'instrument',
                                        'detail': f'close: {error!r}'})
            state['close_completed'] = error is None
            self.terminal[name] = state
            del self.active[name]
        if cancelled:
            raise asyncio.CancelledError
        check_observers({name: state})
        return copy.deepcopy(state)

    async def close_all(self):
        cancelled = False
        for name in list(self.active):
            try:
                await self.close(name)
            except asyncio.CancelledError:
                cancelled = True
            except (InstrumentFailure, MediaFailure):
                # Terminal evidence stores the failure; continue closing siblings.
                pass
        if cancelled:
            raise asyncio.CancelledError
        return self.require_complete()

    def require_complete(self, required=()):
        missing = (self.started | set(required)) - self.terminal.keys()
        if self.active or missing:
            raise InstrumentFailure(f'incomplete observer evidence: {sorted(missing)}')
        check_observers(self.terminal)
        return copy.deepcopy(self.terminal)


def native_window(rows, left, right):
    """Validate the entire declared case scope before selecting usable endpoints."""
    if not isinstance(left, (int, float)) or not isinstance(right, (int, float)):
        raise InstrumentFailure('invalid native window')
    if not math.isfinite(left) or not math.isfinite(right) or left > right:
        raise InstrumentFailure('invalid native window')
    selected = []
    for row in rows:
        try:
            missed, bootstrap, at = row['missed_records'], row['bootstrap_records'], row['wall_ms']
        except (KeyError, TypeError) as exc:
            raise InstrumentFailure('missing native snapshot evidence') from exc
        if (type(missed) is not int or missed < 0 or type(bootstrap) is not int or bootstrap < 0
                or isinstance(at, bool) or not isinstance(at, (int, float)) or not math.isfinite(at)):
            raise InstrumentFailure('invalid native snapshot evidence')
        if missed:
            raise MediaFailure(f'native snapshot has {missed} missed records at {at}')
        if left <= at <= right and bootstrap == 0:
            selected.append(row)
    return selected
