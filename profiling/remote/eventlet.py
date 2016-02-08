# -*- coding: utf-8 -*-
"""
   profiling.remote.eventlet
   ~~~~~~~~~~~~~~~~~~~~~~~~~

   Implements a profiling server based on `eventlet`_.

   .. _eventlet: http://eventlet.net/

   :copyright: (c) 2014-2016, What! Studio
   :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

import socket

import eventlet
from eventlet.semaphore import Semaphore

from . import INTERVAL, LOG, PICKLE_PROTOCOL, ProfilingServer


__all__ = ['EventletProfilingServer']


class StreamServer(object):
    def __init__(self, listen_info, handle=None, backlog=None,
                 spawn='default', **ssl_args):
        assert backlog is None
        assert spawn == 'default'

        if ':' in listen_info[0]:
            self.server = eventlet.listen(listen_info,
                                          family=socket.AF_INET6)
        else:
            self.server = eventlet.listen(listen_info)

        if ssl_args:
            def wrap_and_handle(sock, addr):
                ssl_args.setdefault('server_side', True)
                handle(ssl.wrap_socket(sock, **ssl_args), addr)

            self.handle = wrap_and_handle
        else:
            self.handle = handle

    def serve_forever(self):
        while True:
            sock, addr = self.server.accept()
            spawn(self.handle, sock, addr)


class wrap_errors(object):
    """Helper to make function return an exception, rather than raise it.

    Because every exception that is unhandled by greenlet will be logged,
    it is desirable to prevent non-error exceptions from leaving a greenlet.
    This can done with simple ``try``/``except`` construct::

        def wrapped_func(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except (A, B, C), ex:
                return ex

    :class:`wrap_errors` provides a shortcut to write that in one line::

        wrapped_func = wrap_errors((A, B, C), func)

    It also preserves ``__str__`` and ``__repr__`` of the original function.
    """
    # QQQ could also support using wrap_errors as a decorator

    def __init__(self, errors, func):
        """Make a new function from `func', such that it catches `errors' (an
        Exception subclass, or a tuple of Exception subclasses) and return
        it as a value.
        """
        self.errors = errors
        self.func = func

    def __call__(self, *args, **kwargs):
        func = self.func
        try:
            return func(*args, **kwargs)
        except self.errors, ex:
            return ex

    def __str__(self):
        return str(self.func)

    def __repr__(self):
        return repr(self.func)

    def __getattr__(self, item):
        return getattr(self.func, item)


class EventletProfilingServer(StreamServer, ProfilingServer):
    """A profiling server implementation based on `eventlet`_.  When you choose
    it, you should set a :class:`profiling.timers.greenlet.GreenletTimer` for
    the profiler's timer::

       sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
       sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
       sock.bind(('', 0))
       sock.listen(1)

       profiler = Profiler(GreenletTimer())
       server = EventletProfilingServer(sock, profiler)
       server.serve_forever()

    .. _eventlet: http://eventlet.net/

    """

    def __init__(self, listener, profiler=None, interval=INTERVAL,
                 log=LOG, pickle_protocol=PICKLE_PROTOCOL, **server_kwargs):
        StreamServer.__init__(self, listener, **server_kwargs)
        ProfilingServer.__init__(self, profiler, interval,
                                 log, pickle_protocol)
        self.lock = Semaphore()

    def _send(self, sock, data):
        sock.sendall(data)

    def _close(self, sock):
        sock.close()

    def _addr(self, sock):
        return sock.getsockname()

    def _start_profiling(self):
        eventlet.spawn(self.profile_periodically)

    def _start_watching(self, sock):
        disconnected = lambda x: self.disconnected(sock)
        recv = wrap_errors(socket.error, sock.recv)
        eventlet.spawn(recv, 1).link(disconnected)

    def profile_periodically(self):
        with self.lock:
            for __ in self.profiling():
                eventlet.sleep(self.interval)

    def handle(self, sock, addr=None):
        self.connected(sock)
