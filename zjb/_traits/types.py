from typing import TYPE_CHECKING, Generic, ParamSpec, TypeVar

from traits.trait_types import Any as TraitAny
from traits.trait_types import Callable as _Callable
from traits.trait_types import Enum as TraitEnum
from traits.trait_types import Instance as _Instance

P = ParamSpec("P")
T = TypeVar("T")

OptionalInstance = _Instance  # type: ignore


class TypedInstance(_Instance, Generic[T]):  # type: ignore
    ...


class Instance(_Instance):  # type: ignore
    def __init__(self, klass, *args, **metadata) -> None:
        super().__init__(klass, *args, allow_none=False, **metadata)


OptionalCallable = _Callable


class TraitCallable(_Callable):  # type: ignore
    def __init__(self, *args, **metadata) -> None:
        super().__init__(*args, allow_none=False, **metadata)


class TypedCallable(_Callable, Generic[P, T]):  # type: ignore
    ...


if TYPE_CHECKING:
    from typing import Any, Callable

    from traits.trait_type import _TraitType

    class OptionalInstance(_TraitType[T | None, T | None]):
        def __init__(
            self,
            klass: type[T],
            *args: Any,
            **metadata: Any,
        ):
            ...

    class TypedInstance(_TraitType[T, T], Generic[T]):
        def __init__(
            self,
            klass: Any,
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

    class TypedCallable(_TraitType[Callable[P, T], Callable[P, T]], Generic[P, T]):
        ...
