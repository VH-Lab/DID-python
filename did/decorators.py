from __future__ import annotations
from functools import wraps
import did.types as T

def handle_iter(func: T.Callable[[T.Self, T.Foo], None]) -> T.Callable[[T.Self, T.OneOrManyFoo], None]:
    """
    Decorator: If passed a list of :term:`DID object`\ s, it will call func with each one. Otherwise, it will call func(arg) once.

    :param func: The wrapped function.
    :type func: function

    :param arg: The first argument passed to the wrapped function.
    :type arg: List<:term:`DID object`> | :term:`DID object`

    :rtype: None
    """
    @wraps(func)
    def decorator(self, arg: T.OneOrManyFoo) -> None:
        if isinstance(arg, list):
            for item in arg:
                func(self, item)
        else:
            func(self, arg)
    return decorator


def handle_list(
        func: T.Callable[ [ T.Self, T.Foo ], T.Bar]
    ) -> T.Callable[ [ T.Self, T.OneOrManyFoo], T.OneOrManyBar ]:
    """Decorator: If passed a list of :term:`DID object`\ s, it will call func with each one. Otherwise, it will call func(arg) once.
    The return value of the decorated function corresponds the the argument(s) passed to it:

    ::
        @handle_list
        def func(x): return x * 2

        func(5)
        # returns 10

        func([1,2,3])
        # returns [2,4,6]
    
    :param func: [description]
    :type func: T.Callable[ [ T.Self, T.Foo ], T.Bar]
    :return: [description]
    :rtype: T.Callable[ [ T.Self, T.OneOrManyFoo], T.OneOrManyBar ]
    """
    @wraps(func)
    def decorator(self: T.Self, arg: T.OneOrManyFoo) -> T.OneOrManyBar:
        if isinstance(arg, list):
            return [func(self, item) for item in arg]
        else:
            return func(self, arg)
    return decorator
