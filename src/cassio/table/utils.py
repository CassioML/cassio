import asyncio
from typing import Any, Callable, Dict, List, Optional

from cassandra.cluster import ResponseFuture


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


def handle_multicolumn_unpacking(
    args_dict: Dict[str, Any],
    id_type: List[str],
    key_name: str,
    unpacked_prefix: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Given a dictionary of "args", handle if necessary the replacement of one of its items
    with corresponding split-tuple version if the type structure requires it.
    So if id_type is a one-element list, do nothing.
    If the key_name is None, remove it from the mapping.
    Returns the modified dictionary.

    Example:
        args_dict = {"k": (1, 20), "x": "..."}
        id__type = ["T0", "T1"]
        key_name = "k"
    results in
        {"k_0": 1, "k_1": 20, "x": "..."}

    Example:
        args_dict = {"k": "k_val", "x": "..."}
        id__type = ["T1"]
        key_name = "k"
    results in (unchanged)
        {"k": "k_val", "x": "..."}

    Example:
        args_dict = {"x": "..."}
        args_dict = {"k": None, "x": "..."}
    both result in
        {"x": "..."}
    """
    if unpacked_prefix is None:
        unpacked_prefix = f"{key_name}_"
    if args_dict.get(key_name) is not None:
        if len(id_type) > 1:
            # unpack the tuple
            split_part = {
                f"{unpacked_prefix}{tuple_i}": tuple_v
                for tuple_i, tuple_v in enumerate(args_dict[key_name])
            }
        else:
            split_part = {key_name: args_dict[key_name]}
    else:
        split_part = {}

    new_args_dict = {
        **split_part,
        **{k: v for k, v in args_dict.items() if k != key_name},
    }
    return new_args_dict
