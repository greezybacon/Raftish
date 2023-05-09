# Lightweight asyncio.wait_for implementation
# Borrowed from https://github.com/aio-libs/async-timeout
#
# "Lightweight" in the sense that `wait_for` does not create a new task in the
# event loop but instead creates a callback at the timeout time delta specified,
# cancels the awaited task and raises TimeoutError if the timeout expired.

import asyncio
import contextlib
import enum
from types import TracebackType
from typing import Any, Optional, Type

import logging
log = logging.getLogger('raft.util')

def timeout(delay: Optional[float]) -> "Timeout":
    """timeout context manager.

    Useful in cases when you want to apply timeout logic around block
    of code or in cases when asyncio.wait_for is not suitable. For example:

    >>> async with timeout(0.001):
    ...     async with aiohttp.get('https://github.com') as r:
    ...         await r.text()


    delay - value in seconds or None to disable timeout logic
    """
    loop = asyncio.get_running_loop()
    if delay is not None:
        deadline = loop.time() + delay  # type: Optional[float]
    else:
        deadline = None
    return Timeout(deadline, loop)

def timeout_at(deadline: Optional[float]) -> "Timeout":
    """Schedule the timeout at absolute time.

    deadline arguments points on the time in the same clock system
    as loop.time().

    Please note: it is not POSIX time but a time with
    undefined starting base, e.g. the time of the system power on.

    >>> async with timeout_at(loop.time() + 10):
    ...     async with aiohttp.get('https://github.com') as r:
    ...         await r.text()


    """
    loop = asyncio.get_running_loop()
    return Timeout(deadline, loop)

class _State(enum.Enum):
    INIT = "INIT"
    ENTER = "ENTER"
    TIMEOUT = "TIMEOUT"
    EXIT = "EXIT"

class Timeout:
    # Internal class, please don't instantiate it directly Use timeout() and
    # timeout_at() public factories instead.
    #
    # Implementation note: `async with timeout()` is preferred over `with
    # timeout()`.  While technically the Timeout class implementation doesn't
    # need to be async at all, the `async with` statement explicitly points that
    # the context manager should be used from async function context.
    #
    # This design allows to avoid many silly misusages.
    #
    # TimeoutError is raised immadiatelly when scheduled if the deadline is
    # passed.  The purpose is to time out as sson as possible without waiting
    # for the next await expression.

    __slots__ = ("_deadline", "_loop", "_state", "_task", "_timeout_handler")

    def __init__(
        self, deadline: Optional[float], loop: asyncio.AbstractEventLoop
    ) -> None:
        self._loop = loop
        self._state = _State.INIT

        task = asyncio.current_task(loop=loop)
        self._task = task

        self._timeout_handler = None  # type: Optional[asyncio.Handle]
        if deadline is None:
            self._deadline = None  # type: Optional[float]
        else:
            self.shift_to(deadline)

    async def __aenter__(self) -> "Timeout":
        self._do_enter()
        return self

    async def __aexit__(
        self,
        exc_type: Type[BaseException],
        exc_val: BaseException,
        exc_tb: TracebackType,
    ) -> Optional[bool]:
        self._do_exit(exc_type)
        return exc_type is asyncio.CancelledError

    @property
    def expired(self) -> bool:
        """Is timeout expired during execution?"""
        return self._state == _State.TIMEOUT

    @property
    def deadline(self) -> Optional[float]:
        return self._deadline

    def reject(self) -> None:
        """Reject scheduled timeout if any."""
        # cancel is maybe better name but
        # task.cancel() raises CancelledError in asyncio world.
        if self._state not in (_State.INIT, _State.ENTER):
            raise RuntimeError("invalid state {}".format(self._state.value))
        self._reject()

    def _reject(self) -> None:
        if self._timeout_handler is not None:
            self._timeout_handler.cancel()
            self._timeout_handler = None

    def shift_by(self, delay: float) -> None:
        """Advance timeout on delay seconds.

        The delay can be negative.
        """
        now = self._loop.time()
        self.shift_to(now + delay)

    def shift_to(self, deadline: float) -> None:
        """Advance timeout on the abdelay seconds.

        If new deadline is in the past
        the timeout is raised immediatelly.
        """
        if self._state == _State.EXIT:
            raise RuntimeError("cannot reschedule after exit from context manager")
        if self._state == _State.TIMEOUT:
            raise RuntimeError("cannot reschedule expired timeout")
        if self._timeout_handler is not None:
            self._timeout_handler.cancel()
        self._deadline = deadline
        now = self._loop.time()
        if deadline <= now:
            self._timeout_handler = None
            if self._state == _State.INIT:
                raise asyncio.TimeoutError
            else:
                # state is ENTER
                raise asyncio.CancelledError
        self._timeout_handler = self._loop.call_at(
            deadline, self._on_timeout, self._task
        )

    def _do_enter(self) -> None:
        if self._state != _State.INIT:
            raise RuntimeError("invalid state {}".format(self._state.value))
        self._state = _State.ENTER

    def _do_exit(self, exc_type: Type[BaseException]) -> None:
        if exc_type is asyncio.CancelledError and self._state == _State.TIMEOUT:
            self._timeout_handler = None
            raise asyncio.TimeoutError
        # timeout is not expired
        self._state = _State.EXIT
        self._reject()
        return None

    def _on_timeout(self, task: "asyncio.Task[None]") -> None:
        task.cancel()
        self._state = _State.TIMEOUT

_timeout = timeout
async def wait_for(aw, timeout, *, loop=None):
    async with _timeout(timeout):
        return await aw

asyncio.wait_for = wait_for

@contextlib.asynccontextmanager
async def cancelling(aws):
    aws = {
        asyncio.ensure_future(x)
        for x in aws
    }

    try:
        yield aws
    finally:
        for x in aws:
            log.info("Exiting `cancelling`")
            if not x.done():
                x.cancel()
                await x
            log.info("Exited `cancelling`")

class BroadcastEvent(asyncio.Event):
    def notify_all(self):
        self.set()
        self.clear()

class TaskGroup:

    def __init__(self, wait=all):
        self._entered = False
        self._exiting = False
        self._aborting = False
        self._loop = None
        self._parent_task = None
        self._parent_cancel_requested = False
        self._tasks = set()
        self._errors = []
        self._base_error = None
        self._on_completed_fut = None
        self._wait = wait
        self._done = []

    def __repr__(self):
        info = ['']
        if self._tasks:
            info.append(f'tasks={len(self._tasks)}')
        if self._errors:
            info.append(f'errors={len(self._errors)}')
        if self._aborting:
            info.append('cancelling')
        elif self._entered:
            info.append('entered')

        info_str = ' '.join(info)
        return f'<TaskGroup{info_str}>'

    async def __aenter__(self):
        if self._entered:
            raise RuntimeError(
                f"TaskGroup {self!r} has been already entered")
        self._entered = True

        if self._loop is None:
            self._loop = asyncio.get_running_loop()

        self._parent_task = asyncio.current_task(self._loop)
        if self._parent_task is None:
            raise RuntimeError(
                f'TaskGroup {self!r} cannot determine the parent task')

        return self

    async def __aexit__(self, et, exc, tb):
        self._exiting = True

        if (exc is not None and
                self._is_base_error(exc) and
                self._base_error is None):
            self._base_error = exc

        propagate_cancellation_error = \
            exc if et is asyncio.CancelledError else None
        if self._parent_cancel_requested:
            # If this flag is set we *must* call uncancel().
            if self._parent_task.uncancel() == 0:
                # If there are no pending cancellations left,
                # don't propagate CancelledError.
                propagate_cancellation_error = None

        if et is not None:
            if not self._aborting:
                # Our parent task is being cancelled:
                #
                #    async with TaskGroup() as g:
                #        g.create_task(...)
                #        await ...  # <- CancelledError
                #
                # or there's an exception in "async with":
                #
                #    async with TaskGroup() as g:
                #        g.create_task(...)
                #        1 / 0
                #
                self._abort()
            
            try:
                if self._wait == all:
                    await self.join()
                elif self._wait == any:
                    await anext(self)
            except asyncio.CancelledError as ex:
                # Our parent task is being cancelled:
                #
                #    async def wrapper():
                #        async with TaskGroup() as g:
                #            g.create_task(foo)
                #
                # "wrapper" is being cancelled while "foo" is
                # still running.
                propagate_cancellation_error = ex
                self._abort()

            self._on_completed_fut = None

        assert not self._tasks

        if self._base_error is not None:
            raise self._base_error

        # Propagate CancelledError if there is one, except if there
        # are other errors -- those have priority.
        if propagate_cancellation_error and not self._errors:
            raise propagate_cancellation_error

        if et is not None and et is not asyncio.CancelledError:
            self._errors.append(exc)

        if self._errors:
            # Exceptions are heavy objects that can have object
            # cycles (bad for GC); let's not keep a reference to
            # a bunch of them.
            try:
                me = BaseExceptionGroup('unhandled errors in a TaskGroup', self._errors)
                raise me from None
            finally:
                self._errors = None

    async def __aiter__(self):
        return self

    async def __anext__(self):
        # We use while-loop here because "self._on_completed_fut"
        # can be cancelled multiple times if our parent task
        # is being cancelled repeatedly (or even once, when
        # our own cancellation is already in progress)
        while self._tasks:
            if self._on_completed_fut is None:
                self._on_completed_fut = self._loop.create_future()

            try:
                return await self._on_completed_fut
            except asyncio.CancelledError as ex: 
                if not self._aborting:
                    raise

    async def join(self):
        async for T in self:
            pass

    async def gather(self):
        self._gathering = True
        self.join()

    def create_task(self, coro, *, name=None, context=None):
        if not self._entered:
            raise RuntimeError(f"TaskGroup {self!r} has not been entered")
        if self._exiting and not self._tasks:
            raise RuntimeError(f"TaskGroup {self!r} is finished")
        if self._aborting:
            raise RuntimeError(f"TaskGroup {self!r} is shutting down")
        if context is None:
            task = self._loop.create_task(coro)
        else:
            task = self._loop.create_task(coro, context=context)
        asyncio._set_task_name(task, name)
        task.add_done_callback(self._on_task_done)
        self._tasks.add(task)
        return task

    # Since Python 3.8 Tasks propagate all exceptions correctly,
    # except for KeyboardInterrupt and SystemExit which are
    # still considered special.

    def _is_base_error(self, exc: BaseException) -> bool:
        assert isinstance(exc, BaseException)
        return isinstance(exc, (SystemExit, KeyboardInterrupt))

    def _abort(self):
        self._aborting = True
        self.cancel_remaining()

    def cancel_remaining(self):
        for t in self._tasks:
            if not t.done():
                t.cancel()

    def _on_task_done(self, task):
        self._tasks.discard(task)

        if self._on_completed_fut is not None and not self._tasks:
            if not self._on_completed_fut.done():
                self._on_completed_fut.set_result(task)

        if task.cancelled():
            return

        exc = task.exception()
        if exc is None:
            return

        self._errors.append(exc)
        if self._is_base_error(exc) and self._base_error is None:
            self._base_error = exc

        if self._parent_task.done():
            # Not sure if this case is possible, but we want to handle
            # it anyways.
            self._loop.call_exception_handler({
                'message': f'Task {task!r} has errored out but its parent '
                           f'task {self._parent_task} is already completed',
                'exception': exc,
                'task': task,
            })
            return

        if not self._aborting and not self._parent_cancel_requested:
            # If parent task *is not* being cancelled, it means that we want
            # to manually cancel it to abort whatever is being run right now
            # in the TaskGroup.  But we want to mark parent task as
            # "not cancelled" later in __aexit__.  Example situation that
            # we need to handle:
            #
            #    async def foo():
            #        try:
            #            async with TaskGroup() as g:
            #                g.create_task(crash_soon())
            #                await something  # <- this needs to be canceled
            #                                 #    by the TaskGroup, e.g.
            #                                 #    foo() needs to be cancelled
            #        except Exception:
            #            # Ignore any exceptions raised in the TaskGroup
            #            pass
            #        await something_else     # this line has to be called
            #                                 # after TaskGroup is finished.
            self._abort()
            self._parent_cancel_requested = True
            self._parent_task.cancel()