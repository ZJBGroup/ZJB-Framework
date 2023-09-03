from typing import Any
from typing import Callable as CallableType
from typing import TypeVar

from traits.trait_types import _TraitType

T = TypeVar('T')
_Callable = CallableType[..., Any]


class OptionalInstance(_TraitType[T | None, T | None]):

    def __init__(
        self,
        klass: type[T],
        *args: Any,
        **metadata: Any,
    ): ...


class Instance(_TraitType[T, T]):

    def __init__(
        self,
        klass: type[T],
        *args: Any,
        **metadata: Any,
    ): ...


class OptionalCallable(_TraitType[_Callable | None, _Callable | None]):

    def __init__(
        self,
        value: _Callable | None = None,
        **metadata: Any
    ) -> None: ...


class Callable(_TraitType[_Callable, _Callable]):

    def __init__(
        self,
        value: _Callable | None = None,
        **metadata: Any
    ) -> None: ...
