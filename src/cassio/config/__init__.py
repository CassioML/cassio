import tempfile
import shutil
from typing import Any, Dict, List, Optional, Union

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
    contact_points: Optional[Union[str, List[str]]] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    cluster_kwargs: Optional[Dict[str, Any]] = None,
    tempfile_basedir: Optional[str] = None,
) -> None:
    """
    Globally set the default Cassandra connection (/keyspace) for CassIO.
    This default will be used by all other db-requiring CassIO instantiations,
    unless passed to the respective classes' __init__.

    There are various ways to achieve this, depending on which of the following
    parameters is passed (all optional).
    Broadly speaking, this method allows to pass one's own ready Session,
    or to have it created in the method. For this second case, both Astra DB
    and a regular Cassandra cluster can be targeted.

    CASSANDRA
    If one passes `contact_points`, it is assumed that this is Cassandra.
    In that case, only the following arguments will be used:
    `contact_points`, `keyspace`, `username`, `password`, `cluster_kwargs`
    Note that when passing a `session` all other parameters are ignored.

    ASTRA DB:
    If `contact_points` is not passed, one of several methods to connect to
    Astra should be supplied for the connection to Astra. These overlap:
    see below for their precedence.
    Note that when passing a `session` all other parameters are ignored.

    PARAMETERS:
        `session` (optional cassandra.cluster.Session), an established connection.
        `secure_connect_bundle` (optional str), full path to a Secure Bundle.
        `init_string` (optional str), a stand-alone "db init string" credential
            (which can optionally contain keyspace and/or token).
        `token` (optional str), the Astra DB "AstraCS:..." token.
        `keyspace` (optional str), the keyspace to work.
        `contact_points` (optional List[str]), for Cassandra connection
        `username` (optional str), username for Cassandra connection
        `password` (optional str), password for Cassandra connection
        `cluster_kwargs` (optional dict), additional arguments to `Cluster(...)`.
        `tempfile_basedir` (optional str), where to create temporary work directories.

    ASTRA DB:
    The Astra-related parameters are arranged in a chain of fallbacks.
    In case redundant information is supplied, these are the precedences:
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
        can_be_astra = any(
            [
                secure_connect_bundle is not None,
                init_string is not None,
                token is not None,
            ]
        )
        # resolution of priority among args
        if direct_session:
            default_session = direct_session
        else:
            # first determine if Cassandra or Astra
            is_cassandra = all(
                [
                    secure_connect_bundle is None,
                    init_string is None,
                    token is None,
                    contact_points is not None,
                ]
            )
            if is_cassandra:
                is_astra_db = False
            else:
                # determine if Astra DB
                is_astra_db = can_be_astra
            #
            if is_cassandra:
                # need to take care of:
                #   contact_points, username, password, cluster_kwargs
                chosen_contact_points: Union[List[str], None]
                if contact_points:
                    if isinstance(contact_points, str):
                        chosen_contact_points = [
                            cp.strip() for cp in contact_points.split(",") if cp.strip()
                        ]
                    else:
                        # assume it's a list already
                        chosen_contact_points = contact_points
                else:
                    # normalize "" to None for later `Cluster(...)` call
                    chosen_contact_points = None
                #
                if username is not None and password is not None:
                    chosen_auth_provider = PlainTextAuthProvider(
                        username,
                        password,
                    )
                else:
                    if username is not None or password is not None:
                        raise ValueError(
                            "Please provide both usename/password or none."
                        )
                    else:
                        chosen_auth_provider = None
                #
                if chosen_contact_points is None:
                    cluster = Cluster(
                        auth_provider=chosen_auth_provider,
                        **(cluster_kwargs if cluster_kwargs is not None else {})
                    )
                else:
                    cluster = Cluster(
                        contact_points=chosen_contact_points,
                        auth_provider=chosen_auth_provider,
                        **(cluster_kwargs if cluster_kwargs is not None else {})
                    )
                default_session = cluster.connect()
            elif is_astra_db:
                # Astra DB
                chosen_token = _first_valid(token_from_arg, token_from_is)
                if chosen_token is None:
                    raise ValueError(
                        "A token must be supplied if connection is to be established."
                    )
                chosen_bundle_pre_token = _first_valid(bundle_from_arg, bundle_from_is)
                # TODO: try to get the bundle from the token if not supplied otherwise
                # and re-evaluate chosen_bundle. For now:
                chosen_bundle = chosen_bundle_pre_token
                #
                if chosen_bundle:
                    cluster = Cluster(
                        cloud={"secure_connect_bundle": chosen_bundle},
                        auth_provider=PlainTextAuthProvider(
                            ASTRA_CLOUD_AUTH_USERNAME,
                            chosen_token,
                        ),
                        **(cluster_kwargs if cluster_kwargs is not None else {})
                    )
                    default_session = cluster.connect()
                else:
                    raise ValueError("No secure-connect-bundle was available.")
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
