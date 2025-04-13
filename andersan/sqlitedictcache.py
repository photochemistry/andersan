from collections import OrderedDict
from collections.abc import Callable
from typing import Final, Generic, Hashable, NamedTuple, Optional, ParamSpec, TypeVar
from logging import getLogger

T = TypeVar("T")
P = ParamSpec("P")


# sqlitedict cache
import sqlitedict
import json


class _SQLiteDictCacheFunctionWrapper(Generic[P, T]):
    def __init__(self, func: Callable[P, T], basename: str):
        self.__wrapped__ = func
        self.__basename = basename

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        logger = getLogger()
        call_args = json.dumps(args + tuple(kwargs.items()))
        logger.debug(call_args)
        with sqlitedict.open(f"{self.__basename}.sqlite") as shelf:
            if call_args not in shelf:
                ret = self.__wrapped__(*args, **kwargs)
                if ret is None:
                    logger.info(f"Cache for {self.__basename} prevents storing None.")
                else:
                    shelf[call_args] = ret
                    shelf.commit()
            else:
                logger.debug(f"Cache hit for {self.__basename}.")
                ret = shelf[call_args]
        return ret


def sqlitedict_cache(
    basename: str,
) -> Callable[[Callable[P, T]], _SQLiteDictCacheFunctionWrapper[P, T]]:
    def decorator(func: Callable[P, T]) -> _SQLiteDictCacheFunctionWrapper[P, T]:
        wrapped = _SQLiteDictCacheFunctionWrapper(func, basename)
        wrapped.__doc__ = func.__doc__
        return wrapped

    return decorator


def cache_if_not_none(func):
    @functools.wraps(func)
    @sqlitedict_cache
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        if result is None:
            wrapper.__wrapped__.__cache__.pop(
                (args, tuple(kwargs.items())), None
            )  # キャッシュから削除
        return result

    return wrapper


@sqlitedict_cache("fib")
def fib(n):
    return 1 if n in (0, 1) else fib(n - 1) + fib(n - 2)


if __name__ == "__main__":
    print(fib(40))
