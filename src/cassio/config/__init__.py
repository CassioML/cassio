import tempfile
import shutil
from typing import Any, Dict, Optional, Union

from cassandra.cluster import Cluster, Session  # type: ignore
from cassandra.auth import PlainTextAuthProvider  # type: ignore

from cassio.config.bundle_management import init_string_to_bundle_path_and_options


ASTRA_CLOUD_AUTH_USERNAME = "token"

default_session: Optional[Session] = None
default_keyspace: Optional[str] = None


def init(
    session: Optional[Session] = None,
    secure_connect_bundle: Optional[str] = None,
    init_string: Optional[str] = None,
    token: Optional[str] = None,
    keyspace: Optional[str] = None,
    cluster_kwargs: Optional[Dict[str, Any]] = None,
    tempfile_basedir: Optional[str] = None,
) -> None:
    """
    Globally set the default Cassandra connection (/keyspace) for CassIO.
    This default will be used by all other CassIO instantiations, unless
    passed at instantiation time there.

    There are various ways to achieve this, depending on which of the following
    parameters is passes (all optional).
        `session` (optional cassandra.cluster.Session), an established connection.
        `secure_connect_bundle` (optional str), full path to a Secure Bundle.
        `init_string` (optional str), a stand-alone "db init string" credential
            (which can optionally contain keyspace and/or token).
        `token` (optional str), the Astra DB "AstraCS:..." token.
        `keyspace` (optional str), the keyspace to work.
        `cluster_kwargs` (optional dict), additional arguments to `Cluster(...)`.
        `tempfile_basedir` (optional str), where to create temporary work directories.

    The above parameters are arranged in a chain of fallbacks,
    just in case redundant information is supplied:
        session > secure_connect_bundle > init_string
        token > (from init_string if any)
        keyspace > (from init_string if any)

    Constraints and caveats:
        `secure_connect_bundle` requires `token`.
        `session` does not make use of `cluster_kwargs` and will ignore it.

    The Session is created at `init` time and kept around, available to any
    subsequent table creation. If calling `init` a second time, a new Session
    will be made available replacing the previous one.
    """
    global default_session
    global default_keyspace
    temp_dir_created: bool = False
    temp_dir: Optional[str] = None
    direct_session: Optional[Session] = None
    bundle_from_is: Optional[str] = None
    bundle_from_arg: Optional[str] = None
    keyspace_from_is: Optional[str] = None
    keyspace_from_arg: Optional[str] = None
    token_from_is: Optional[str] = None
    token_from_arg: Optional[str] = None
    #
    try:
        # process init_string
        if init_string:
            base_dir = tempfile_basedir if tempfile_basedir else tempfile.gettempdir()
            temp_dir = tempfile.mkdtemp(dir=base_dir)
            temp_dir_created = True
            bundle_from_is, options_from_is = init_string_to_bundle_path_and_options(
                init_string,
                target_dir=temp_dir,
            )
            keyspace_from_is = options_from_is.get("keyspace")
            token_from_is = options_from_is.get("token")
        # for the session
        if session:
            direct_session = session
        if secure_connect_bundle:
            if not token:
                raise ValueError(
                    "`token` is required if `secure_connect_bundle` is passed"
                )
        # params from arguments:
        bundle_from_arg = secure_connect_bundle
        token_from_arg = token
        keyspace_from_arg = keyspace
        # resolution of priority among args
        if direct_session:
            default_session = direct_session
        else:
            chosen_bundle = _first_valid(bundle_from_arg, bundle_from_is)
            if chosen_bundle:
                chosen_token = _first_valid(token_from_arg, token_from_is)
                if chosen_token is None:
                    raise ValueError(
                        "A token must be supplied if connection is to be established."
                    )
                cluster = Cluster(
                    cloud={"secure_connect_bundle": chosen_bundle},
                    auth_provider=PlainTextAuthProvider(
                        ASTRA_CLOUD_AUTH_USERNAME,
                        chosen_token,
                    ),
                    **(cluster_kwargs if cluster_kwargs is not None else {})
                )
                default_session = cluster.connect()
        # keyspace to be resolved in any case
        chosen_keyspace = _first_valid(keyspace_from_arg, keyspace_from_is)
        default_keyspace = chosen_keyspace
    finally:
        if temp_dir_created and temp_dir is not None:
            shutil.rmtree(temp_dir)


def resolve_session(arg_session: Optional[str] = None) -> Optional[Session]:
    """Utility to fall back to the global defaults when null args are passed."""
    if arg_session is not None:
        return arg_session
    else:
        return default_session


def check_resolve_session(arg_session: Optional[str] = None) -> Session:
    s = resolve_session(arg_session)
    if s is None:
        raise ValueError("DB session not set.")
    else:
        return s


def resolve_keyspace(arg_keyspace: Optional[str] = None) -> Optional[str]:
    """Utility to fall back to the global defaults when null args are passed."""
    if arg_keyspace is not None:
        return arg_keyspace
    else:
        return default_keyspace


def check_resolve_keyspace(arg_keyspace: Optional[str] = None) -> str:
    s = resolve_keyspace(arg_keyspace)
    if s is None:
        raise ValueError("DB keyspace not set.")
    else:
        return s


def _first_valid(*pargs: Optional[Any]) -> Union[Any, None]:
    for entry in pargs:
        if entry is not None:
            return entry
    return None
