import sys
from unittest.mock import Mock

import pytest

from celery.utils import green
from celery.utils.green import cooperative_yield


class test_cooperative_yield:

    # ``cooperative_yield`` restores the one cooperative scheduling point that
    # pooled broker-resource acquisition never had, so that a greenlet
    # publishing in a loop stops starving its peers (celery/celery#10044).

    @pytest.fixture(autouse=True)
    def reset_yield(self):
        # The positive verdict is cached in a module global, so it is cleared
        # on both sides of every test to keep this module order-independent.
        green._yield = None
        yield
        green._yield = None

    def _patch_gevent(self, monkeypatch, detect, sleep=None):
        # ``cooperative_yield`` resolves ``gevent`` and ``kombu.utils.compat``
        # with function-local imports at call time, so the source modules --
        # never ``celery.utils.green`` -- are the only valid patch targets.
        self.patching.modules('gevent')
        monkeypatch.setattr('kombu.utils.compat._detect_environment', detect)
        sleep = Mock() if sleep is None else sleep
        monkeypatch.setattr('gevent.sleep', sleep)
        return sleep

    def test_is_a_noop_when_gevent_was_never_imported(self, monkeypatch):
        # The state of every prefork, solo and threads deployment: the
        # ``sys.modules`` guard short-circuits before importing anything.
        monkeypatch.delitem(sys.modules, 'gevent', raising=False)
        assert cooperative_yield() is False
        assert green._yield is None

    def test_is_a_noop_when_the_environment_is_not_gevent(self, monkeypatch):
        # kombu probes for eventlet before gevent, so an eventlet process can
        # never take the gevent branch and is left untouched.
        detect = Mock(return_value='default')
        sleep = self._patch_gevent(monkeypatch, detect)
        assert cooperative_yield() is False
        detect.assert_called_once_with()
        sleep.assert_not_called()
        assert green._yield is None

    def test_yields_to_the_hub_under_gevent(self, monkeypatch):
        sleep = self._patch_gevent(monkeypatch, Mock(return_value='gevent'))
        assert cooperative_yield() is True
        sleep.assert_called_once_with(0)

    def test_caches_the_positive_verdict(self, monkeypatch):
        # Caching optimises the environment probe only: the hub round trip
        # itself must still happen on every single call.
        detect = Mock(return_value='gevent')
        sleep = self._patch_gevent(monkeypatch, detect)
        assert cooperative_yield() is True
        assert cooperative_yield() is True
        detect.assert_called_once_with()
        assert sleep.call_count == 2

    def test_does_not_cache_the_negative_verdict(self, monkeypatch):
        # ``monkey.patch_all()`` may legitimately run after the first call,
        # e.g. under ``gunicorn -k gevent``, and must still be honoured.
        detect = Mock(side_effect=['default', 'gevent'])
        sleep = self._patch_gevent(monkeypatch, detect)
        assert cooperative_yield() is False
        assert cooperative_yield() is True
        assert detect.call_count == 2
        sleep.assert_called_once_with(0)

    def test_propagates_exceptions_raised_by_the_hub(self, monkeypatch):
        # The yield is called bare, ahead of any resource acquisition, so an
        # interrupt must surface unchanged and leave no resource checked out.
        exc = RuntimeError('Semaphore released too many times')
        self._patch_gevent(monkeypatch, Mock(return_value='gevent'),
                           Mock(side_effect=exc))
        with pytest.raises(RuntimeError) as einfo:
            cooperative_yield()
        assert einfo.value is exc
