from typing import Any, Callable, Generic, ParamSpec, Protocol, Self, TypeVar, overload

O_co = TypeVar("O_co", contravariant=True)
P = ParamSpec("P")
R_co = TypeVar("R_co", covariant=True)

class Method(Protocol[O_co, P, R_co]):
    """A method is a callable define within the class


    Pyright currently cannot correctly handle in-class methods decorated
    with wraps and other decorators, by define __get__, we can make the
    type checker recognize a Callable as an in-class method, so the type
    checker works as expected, and Pylance provides correct autocompletion
    """

    # `_self` is the instance of the method itself
    # `self` means a instance of class `O` in which the method `_self` is defined
    def __call__(_self, self: O_co, *args: P.args, **kwds: P.kwargs) -> R_co: ...

    # for cls.method, the method is still itself
    @overload
    def __get__(_self, self: None, cls: type[O_co]) -> Self: ...

    # for self.method, the method degenerates into a callable without its first parameter `self`
    @overload
    def __get__(_self, self: O_co, cls: type[O_co]) -> Callable[P, R_co]: ...
    def __get__(
        _self, self: O_co | None, cls: type[O_co]
    ) -> Self | Callable[P, R_co]: ...
