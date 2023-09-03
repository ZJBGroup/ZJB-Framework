from traits.trait_types import Callable as _Callable
from traits.trait_types import Instance as _Instance


class OptionalInstance(_Instance):
    """Instance(..., allow_none=True, ...)"""

    def __init__(
        self, klass, factory=None, args=None, kw=None,
        adapt=None, module=None, **metadata
    ):
        super().__init__(
            klass, factory=factory, args=args, kw=kw,
            allow_none=True, adapt=adapt, module=module,
            **metadata)


class Instance(_Instance):
    """Instance(..., allow_none=False, ...)"""

    def __init__(
        self, klass, factory=None, args=None, kw=None,
        adapt=None, module=None, **metadata
    ):
        super().__init__(
            klass, factory=factory, args=args, kw=kw,
            allow_none=False, adapt=adapt, module=module,
            **metadata)


class OptionalCallable(_Callable):
    """Callable(..., allow_none=True, ...)"""

    def __init__(self, value=None, **metadata):
        super().__init__(value, allow_none=True, **metadata)


class Callable(_Callable):
    """Callable(..., allow_none=False, ...)"""

    def __init__(self, value=None, **metadata):
        super().__init__(value, allow_none=False, **metadata)
