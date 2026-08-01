import threading
from unittest.mock import MagicMock, Mock, patch

from celery.concurrency.gevent import TaskPool, Timer, apply_timeout
from celery.utils import green

gevent_modules = (
    'gevent',
    'gevent.greenlet',
    'gevent.monkey',
    'gevent.pool',
    'gevent.signal',
)


class test_gevent_patch:

    def test_is_patched(self):
        self.patching.modules(*gevent_modules)
        patch_all = self.patching('gevent.monkey.patch_all')
        import gevent
        gevent.version_info = (1, 0, 0)
        from celery import maybe_patch_concurrency
        maybe_patch_concurrency(['x', '-P', 'gevent'])
        patch_all.assert_called()


class test_Timer:

    def setup_method(self):
        self.patching.modules(*gevent_modules)
        self.greenlet = self.patching('gevent.greenlet')
        self.GreenletExit = self.patching('gevent.greenlet.GreenletExit')

    def test_sched(self):
        self.greenlet.Greenlet = object
        x = Timer()
        self.greenlet.Greenlet = Mock()
        x._Greenlet.spawn_later = Mock()
        x._GreenletExit = KeyError
        entry = Mock()
        g = x._enter(1, 0, entry)
        assert x.queue

        x._entry_exit(g)
        g.kill.assert_called_with()
        assert not x._queue

        x._queue.add(g)
        x.clear()
        x._queue.add(g)
        g.kill.side_effect = KeyError()
        x.clear()

        g = x._Greenlet()
        g.cancel()


class test_TaskPool:

    def setup_method(self):
        self.patching.modules(*gevent_modules)
        self.spawn_raw = self.patching('gevent.spawn_raw')
        self.Pool = self.patching('gevent.pool.Pool')

    def test_pool(self):
        x = TaskPool()
        x.on_start()
        x.on_stop()
        x.on_apply(Mock())
        x._pool = None
        x.on_stop()

        x._pool = Mock()
        x._pool._semaphore.counter = 1
        x._pool.size = 1
        x.grow()
        assert x._pool.size == 2
        assert x._pool._semaphore.counter == 2
        x.shrink()
        assert x._pool.size, 1
        assert x._pool._semaphore.counter == 1

        x._pool = [4, 5, 6]
        assert x.num_processes == 3

    def test_terminate_job(self):
        func = Mock()
        pool = TaskPool(10)
        pool.on_start()
        pool.on_apply(func)

        assert len(pool._pool_map.keys()) == 1
        pid = list(pool._pool_map.keys())[0]
        greenlet = pool._pool_map[pid]
        greenlet.link.assert_called_once()

        pool.terminate_job(pid)
        import gevent

        gevent.kill.assert_called_once()

    def test_make_killable_target(self):
        def valid_target():
            return "some result..."

        def terminating_target():
            from greenlet import GreenletExit
            raise GreenletExit

        assert TaskPool._make_killable_target(valid_target)() == "some result..."
        assert TaskPool._make_killable_target(terminating_target)() == (False, None, None)

    def test_cleanup_after_job_finish(self):
        testMap = {'1': None}
        TaskPool._cleanup_after_job_finish(None, testMap, '1')
        assert len(testMap) == 0


class test_apply_timeout:

    def test_apply_timeout(self):
        self.patching.modules(*gevent_modules)

        class Timeout(Exception):
            value = None

            def __init__(self, value):
                self.__class__.value = value

            def __enter__(self):
                return self

            def __exit__(self, *exc_info):
                pass
        timeout_callback = Mock(name='timeout_callback')
        apply_target = Mock(name='apply_target')
        getpid = Mock(name='getpid')
        apply_timeout(
            Mock(), timeout=10, callback=Mock(name='callback'),
            timeout_callback=timeout_callback, getpid=getpid,
            apply_target=apply_target, Timeout=Timeout,
        )
        assert Timeout.value == 10
        apply_target.assert_called()

        apply_target.side_effect = Timeout(10)
        apply_timeout(
            Mock(), timeout=10, callback=Mock(),
            timeout_callback=timeout_callback, getpid=getpid,
            apply_target=apply_target, Timeout=Timeout,
        )
        timeout_callback.assert_called_with(False, 10)


class test_cooperative_publishing:
    # Regression guard for celery/celery#10044.  Once the producer pool is
    # warm every step of a publish is non-blocking -- the LIFO hands back an
    # idle slot immediately, the declaration comes from the per-connection
    # cache, and the frame is written with a fire-and-forget sendall -- so
    # nothing switched to the gevent hub and a greenlet publishing in a loop
    # starved every peer.  These constants mirror the acceptance oracle: five
    # publishers, ten publishes each, and the mean number of distinct
    # publishers per ten-wide sliding window as the metric.
    PUBLISHERS = 5
    ITERATIONS = 10
    WINDOW = 10
    # Hang guard only.  The baton is handed on in microseconds while the ring
    # is intact, so this never fires; it exists so that a stalled ring fails
    # loudly and immediately instead of blocking the suite.  Nothing about the
    # measured property depends on wall-clock time.
    RESCUE_TIMEOUT = 5.0

    def setup_method(self):
        self.patching.modules(*gevent_modules)
        # kombu's probe compares ``socket.socket`` against
        # ``gevent.socket.socket``, an identity check a synthesized module can
        # never satisfy, so the verdict has to be supplied here for
        # ``cooperative_yield`` to reach its yield.
        self.detect_environment = self.patching(
            'kombu.utils.compat._detect_environment')
        self.detect_environment.return_value = 'gevent'
        # ``cooperative_yield`` caches a direct reference to ``gevent.sleep``
        # and neither monkeypatch nor the module synthesis clears it, so the
        # cache must be reset on both sides of every test to keep this module
        # order-independent and to stop a stale mock leaking into any later
        # test that reaches a pooled acquisition.
        green._yield = None

    def teardown_method(self):
        green._yield = None

    def _windows(self, seq):
        return [seq[i:i + self.WINDOW] for i in range(len(seq) - self.WINDOW + 1)]

    def _effective_concurrency(self, seq):
        windows = self._windows(seq)
        return sum(len(set(window)) for window in windows) / len(windows)

    def _has_solo_block(self, seq):
        return any(len(set(window)) == 1 for window in self._windows(seq))

    def _publisher_pool(self):
        # ``FallbackContext.__enter__`` calls ``__enter__`` on whatever the
        # acquisition returns, and a plain Mock does not implement the
        # context-manager protocol, so the double has to be a MagicMock.
        return MagicMock(name='producer_pool')

    def _drive(self, sleep):
        # Deterministic token ring standing in for the gevent scheduler:
        # exactly one publisher is runnable at any instant, and the baton only
        # moves when the code under test reaches the patched ``gevent.sleep``
        # or when a publisher retires.  No broker, no real hub, no timing.
        emitted = []
        errors = []
        batons = [threading.Event() for _ in range(self.PUBLISHERS)]
        finished = [False] * self.PUBLISHERS
        aborting = threading.Event()

        def next_active(index):
            for offset in range(1, self.PUBLISHERS):
                candidate = (index + offset) % self.PUBLISHERS
                if not finished[candidate]:
                    return candidate
            return None

        def whoami():
            return int(threading.current_thread().name.rsplit('-', 1)[1])

        def park(index):
            # Fail closed.  ``Event.wait`` returns False when it timed out,
            # which means the baton never arrived and this publisher does not
            # own the ring; resuming anyway would let two publishers emit at
            # once and can fabricate the healthy signature the code under test
            # never produced -- a stranded publisher measures 5.00 with no solo
            # block, so the oracle assertions cannot be relied on to catch it.
            # The stall is therefore turned into an error that ``publish``
            # records and ``_drive`` re-raises once every thread has been
            # joined, and the baton is cleared only once it has been owned.
            if not batons[index].wait(timeout=self.RESCUE_TIMEOUT):
                raise TimeoutError(
                    f'token ring stalled: publisher {index} waited '
                    f'{self.RESCUE_TIMEOUT}s for a baton that never arrived'
                )
            batons[index].clear()
            if aborting.is_set():
                # Released by the rescue path below rather than by a peer
                # handing the baton on, so ownership is just as broken.
                raise RuntimeError(
                    f'token ring aborted: publisher {index} was released '
                    'without receiving the baton'
                )

        def hand_off(index):
            successor = next_active(index)
            if successor is not None:
                batons[successor].set()
            return successor

        def hub(*args, **kwargs):
            # Hand the baton to the next publisher still running, then park
            # until it comes back.  The last survivor must not park, or it
            # would wait for a peer that can never hand off to it again.
            index = whoami()
            if aborting.is_set() or hand_off(index) is None:
                return
            park(index)

        sleep.side_effect = hub

        def publish(index):
            try:
                park(index)
                for _ in range(self.ITERATIONS):
                    with self.app.producer_or_acquire():
                        emitted.append(index)
            except BaseException as exc:
                errors.append(exc)
            finally:
                # Load-bearing: without this hand-off the ring stalls the
                # moment a publisher retires, and the negative control -- in
                # which the patched sleep is never reached -- would never
                # progress past its first publisher.
                finished[index] = True
                hand_off(index)

        threads = [
            threading.Thread(
                target=publish, args=(index,),
                name=f'cooperative-publisher-{index}',
            )
            for index in range(self.PUBLISHERS)
        ]
        try:
            for thread in threads:
                thread.start()
            batons[0].set()
            for thread in threads:
                thread.join(timeout=self.RESCUE_TIMEOUT)
        finally:
            # Rescue path.  Inert while the ring is intact, but it guarantees
            # no publisher can outlive the test even if one stalls, because
            # ``threads_not_lingering`` fails the test case otherwise.  A
            # publisher woken from a park by this path reports the broken
            # ownership instead of resuming, so a stall can never pass.
            aborting.set()
            for baton in batons:
                baton.set()
            for thread in threads:
                thread.join(timeout=self.RESCUE_TIMEOUT)
        for thread in threads:
            assert not thread.is_alive()
        if errors:
            raise errors[0]
        return emitted

    def test_yields_to_the_hub_before_acquiring_from_the_pool(self):
        sleep = self.patching('gevent.sleep')
        pool = self._publisher_pool()
        recorder = Mock(name='recorder')
        recorder.attach_mock(sleep, 'sleep')
        recorder.attach_mock(pool.acquire, 'acquire')
        with patch.object(
            type(self.app), 'producer_pool',
            new_callable=lambda: property(lambda self: pool)
        ):
            producer = self.app._acquire_producer(timeout=None)
        # Ordering is the whole point: yielding after the acquisition would
        # hold a checked-out resource across the hub round trip and would not
        # break the LIFO barging cycle that starves the notified waiter.
        assert [record[0] for record in recorder.mock_calls] == ['sleep', 'acquire']
        sleep.assert_called_once_with(0)
        pool.acquire.assert_called_once_with(block=True, timeout=None)
        assert producer is pool.acquire.return_value

    def test_yields_to_the_hub_before_acquiring_a_pooled_connection(self):
        # The pooled connection path has the identical defect, and starves
        # peers the same way for pooled ``app.control`` operations.
        self.app.conf.broker_pool_acquire_timeout = 30
        sleep = self.patching('gevent.sleep')
        pool = self._publisher_pool()
        recorder = Mock(name='recorder')
        recorder.attach_mock(sleep, 'sleep')
        recorder.attach_mock(pool.acquire, 'acquire')
        with patch.object(
            type(self.app), 'pool',
            new_callable=lambda: property(lambda self: pool)
        ):
            connection = self.app._acquire_connection(pool=True)
        assert [record[0] for record in recorder.mock_calls] == ['sleep', 'acquire']
        sleep.assert_called_once_with(0)
        pool.acquire.assert_called_once_with(block=True, timeout=30)
        assert connection is pool.acquire.return_value

    def test_does_not_yield_when_no_pool_is_used(self):
        # The gevent environment is armed by ``setup_method`` exactly as it is
        # for the two tests above, so this cannot pass vacuously: it fails if
        # the yield is ever hoisted out of the ``if pool:`` branch, which is
        # what keeps the non-pooled callers unaffected.
        sleep = self.patching('gevent.sleep')
        with patch.object(self.app, 'connection_for_write') as connection_for_write:
            connection = self.app._acquire_connection(pool=False)
        connection_for_write.assert_called_once_with()
        assert connection is connection_for_write.return_value
        sleep.assert_not_called()

    def test_concurrent_publishers_interleave(self):
        sleep = self.patching('gevent.sleep')
        pool = self._publisher_pool()
        with patch.object(
            type(self.app), 'producer_pool',
            new_callable=lambda: property(lambda self: pool)
        ):
            emitted = self._drive(sleep)
        assert len(emitted) == self.PUBLISHERS * self.ITERATIONS
        # A perfect round robin scores exactly PUBLISHERS on the oracle's
        # metric, which is the healthy signature the fix restores.
        assert self._effective_concurrency(emitted) == float(self.PUBLISHERS)
        assert not self._has_solo_block(emitted)
        assert pool.acquire.call_count == self.PUBLISHERS * self.ITERATIONS
        assert sleep.call_count == self.PUBLISHERS * self.ITERATIONS

    def test_publishers_run_sequentially_without_the_yield(self):
        sleep = self.patching('gevent.sleep')
        pool = self._publisher_pool()
        # Negative control.  Disabling the yield must reinstate the reported
        # failure through the identical harness, which is what makes the test
        # above a regression guard rather than a tautology.
        with patch('celery.app.base.cooperative_yield', return_value=False):
            with patch.object(
                type(self.app), 'producer_pool',
                new_callable=lambda: property(lambda self: pool)
            ):
                emitted = self._drive(sleep)
        assert len(emitted) == self.PUBLISHERS * self.ITERATIONS
        assert self._effective_concurrency(emitted) <= 3.0
        assert self._has_solo_block(emitted)
        assert pool.acquire.call_count == self.PUBLISHERS * self.ITERATIONS
        sleep.assert_not_called()
