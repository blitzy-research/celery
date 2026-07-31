"""Cooperative scheduling helpers for the green execution pools.

Greenlets are scheduled cooperatively: a greenlet keeps running until it calls
something that switches to the gevent hub.  A code path that performs no
blocking operation therefore starves every other runnable greenlet, however
many of them are ready to run.  See
https://www.gevent.org/intro.html#cooperative-multitasking.
"""
import sys

__all__ = ('cooperative_yield',)

#: Bound to :func:`gevent.sleep` the first time a monkey-patched gevent
#: environment is confirmed.  Only the *positive* verdict is cached:
#: ``gevent.monkey.patch_all()`` may not have run yet the first time this is
#: called, and it must still be honoured once it does.
_yield = None


def cooperative_yield():
    """Give the other greenlets a chance to run, when running under gevent.

    Returns:
        bool: :const:`True` if control was yielded to the gevent hub,
            :const:`False` when not running in a monkey-patched gevent
            environment, in which case this is a no-op.
    """
    global _yield
    if _yield is None:
        # Cheapest possible guard for the non-green case: gevent cannot have
        # patched anything if it was never imported.
        if 'gevent' not in sys.modules:
            return False
        # ``_detect_environment`` is the uncached variant; the public
        # ``detect_environment`` memoizes and may have been primed before
        # monkey-patching happened.
        from kombu.utils.compat import _detect_environment
        if _detect_environment() != 'gevent':
            return False
        from gevent import sleep
        _yield = sleep
    # ``sleep(0)`` is gevent's documented cooperative yield: it switches to the
    # hub so every other runnable greenlet proceeds, without adding any delay.
    _yield(0)
    return True
