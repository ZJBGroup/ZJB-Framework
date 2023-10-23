from typing import TYPE_CHECKING

from traits.trait_types import Any as TraitAny
from traits.trait_types import Callable as _Callable
from traits.trait_types import Enum as TraitEnum
from traits.trait_types import Instance as _Instance

OptionalInstance = _Instance  # type: ignore

StrInstance = _Instance  # type: ignore


class Instance(_Instance):  # type: ignore
    def __init__(self, klass, *args, **metadata) -> None:
        super().__init__(klass, *args, allow_none=False, **metadata)


OptionalCallable = _Callable


class TraitCallable(_Callable):  # type: ignore
    def __init__(self, *args, **metadata) -> None:
        super().__init__(*args, allow_none=False, **metadata)


if TYPE_CHECKING:
    from typing import Any, Callable, TypeVar

    from traits.trait_type import _TraitType

    T = TypeVar("T")

    class OptionalInstance(_TraitType[T | None, T | None]):
        def __init__(
            self,
            klass: type[T],
            *args: Any,
            **metadata: Any,
        ):
            ...

    class StrInstance(_TraitType[Any, Any]):
        def __init__(
            self,
            klass: str,
            *args: Any,
            **metadata: Any,
        ):
            ...

    class Instance(_TraitType[T, T]):
        def __init__(
            self,
            klass: type[T],
            *args: Any,
            **metadata: Any,
        ):
            ...

    class TraitCallable(_TraitType[Callable, Callable]):
        ...
