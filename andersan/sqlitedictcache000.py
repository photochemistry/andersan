from collections.abc import Callable
from typing import Generic, Hashable, NamedTuple, Optional, ParamSpec, TypeVar
from logging import getLogger
import threading  # スレッド関連の処理を追加
import json
import functools
import sqlitedict

T = TypeVar("T")
P = ParamSpec("P")


class _SQLiteDictCacheFunctionWrapper(Generic[P, T]):
    def __init__(self, func: Callable[P, T], basename: str):
        self.__wrapped__ = func
        self.__basename = basename
        self._locks = {}  # 引数ごとのロックを格納する辞書
        self._db_lock = threading.Lock() #データベース全体へのロック

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        logger = getLogger()
        call_args = json.dumps(args + tuple(kwargs.items()))

        # 引数に対応するロックを取得（なければ作成）
        with self._db_lock:
            if call_args not in self._locks:
                self._locks[call_args] = threading.Lock()
            lock = self._locks[call_args]

        with lock:  # ロックを取得
            logger.debug(f"Lock acquired for {call_args}")
            with sqlitedict.open(f"{self.__basename}.sqlite") as shelf:
                if call_args not in shelf:
                    logger.info(f"Cache miss for {call_args}")
                    ret = self.__wrapped__(*args, **kwargs)
                    if ret is None:
                        logger.info(f"Cache for {self.__basename} prevents storing None.")
                    else:
                        shelf[call_args] = ret
                        shelf.commit()
                else:
                    logger.info(f"Cache hit for {call_args}")
                    ret = shelf[call_args]
            logger.debug(f"Lock released for {call_args}")
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
    import time
    time.sleep(3)
    return 1 if n in (0, 1) else fib(n - 1) + fib(n - 2)


if __name__ == "__main__":
    import logging
    import concurrent.futures

    logging.basicConfig(level=logging.DEBUG)

    # 同時に複数の処理を実行する
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        # 同じ引数で複数回呼び出す
        futures = [executor.submit(fib, 10) for _ in range(5)]
        for future in concurrent.futures.as_completed(futures):
            print(f"Result: {future.result()}")
