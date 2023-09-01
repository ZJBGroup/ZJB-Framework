from typing import Any, TypeVar

from traits.trait_types import _TraitType

T = TypeVar('T')


class Instance(_TraitType[T, T]):

    # simplified signature
    def __init__(
        self,
        klass: type[T],
        *args: Any,
        **metadata: Any,
    ) -> None:
        ...
