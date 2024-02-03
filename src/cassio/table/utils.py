import asyncio
from typing import Callable, Any

from cassandra.cluster import ResponseFuture  # type: ignore


async def call_wrapped_async(
    func: Callable[..., ResponseFuture], *args: Any, **kwargs: Any
) -> Any:
    loop = asyncio.get_event_loop()
    asyncio_future = loop.create_future()
    response_future = func(*args, **kwargs)

    def success_handler(_: Any) -> None:
        loop.call_soon_threadsafe(asyncio_future.set_result, response_future.result())

    def error_handler(exc: BaseException) -> None:
        loop.call_soon_threadsafe(asyncio_future.set_exception, exc)

    response_future.add_callbacks(success_handler, error_handler)
    return await asyncio_future
