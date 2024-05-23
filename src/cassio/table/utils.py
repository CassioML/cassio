import asyncio
from typing import Any, Callable, Dict, Iterable

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
    key_name: str,
    unpacked_keys: Iterable[str],
) -> Dict[str, Any]:
    """
    Given a dictionary of "args", handle if necessary the replacement of one of its items
    with corresponding split-tuple version if the type structure requires it.
    So if unpacked_keys is == [key_name], do nothing.
    If the key_name is None, remove it from the mapping altogether.
    Returns the modified dictionary.

    Example:
        args_dict = {"k": (1, 20), "x": "..."}
        key_name = "k"
        unpacked_keys = ["k_0", "k_1"]
    results in
        {"k_0": 1, "k_1": 20, "x": "..."}

    Example:
        args_dict = {"k": "k_val", "x": "..."}
        key_name = "k"
        unpacked_keys = ["k"]
    results in (unchanged)
        {"k": "k_val", "x": "..."}

    Example:
        args_dict = {"x": "..."}
        args_dict = {"k": None, "x": "..."}
    both result in
        {"x": "..."}
    """
    _unp_keys = list(unpacked_keys)
    if args_dict.get(key_name) is not None:
        if _unp_keys != [key_name]:
            # passing a longer tuple than the keys is meaningless:
            assert len(_unp_keys) >= len(args_dict[key_name])
            # unpack the tuple
            split_part = {
                unp_k: tuple_v for unp_k, tuple_v in zip(_unp_keys, args_dict[key_name])
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


def handle_multicolumn_packing(
    unpacked_row: Dict[str, Any],
    key_name: str,
    unpacked_keys: Iterable[str],
) -> Dict[str, Any]:
    _unp_keys = list(unpacked_keys)
    if _unp_keys != [key_name]:
        packed_keys = {k: v for k, v in unpacked_row.items() if k in _unp_keys}
        if packed_keys == {}:
            return unpacked_row
        else:
            pk_tuple = tuple(packed_keys[pk_k] for pk_k in _unp_keys)
            packed_row_portion = {
                key_name: pk_tuple,
            }
            return {
                **packed_row_portion,
                **{k: v for k, v in unpacked_row.items() if k not in _unp_keys},
            }
    else:
        return unpacked_row
