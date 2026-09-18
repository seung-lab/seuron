# -*- coding: utf-8 -*-

import os

import requests
from urllib3.util.retry import Retry

from airflow_client.client import ApiClient, Configuration
from airflow_client.client.api import (
    ConnectionApi,
    DAGApi,
    DagRunApi,
    TaskInstanceApi,
    VariableApi,
)
from airflow_client.client.exceptions import (
    ApiException,
    ConflictException,
    ForbiddenException,
    NotFoundException,
    UnauthorizedException,
)
from airflow_client.client.models import (
    ConnectionBody,
    DAGPatchBody,
    DAGRunPatchBody,
    PatchTaskInstanceBody,
    TriggerDAGRunPostBody,
    VariableBody,
)

API_BASE = os.environ.get("AIRFLOW_API_BASE_URL", "http://webserver:8080/airflow").rstrip("/")
LOGIN_PATH = API_BASE + "/auth/token"
USER = os.environ.get("_AIRFLOW_WWW_USER_USERNAME", "airflow")
PASSWORD = os.environ.get("_AIRFLOW_WWW_USER_PASSWORD", "airflow")

# Retry transient failures (429/5xx) with backoff. urllib3's default
# allowed_methods excludes PATCH, so state changes are never replayed —
# only the GET list/detail calls are.
_RETRY = Retry(total=3, connect=3, read=3, status=3, backoff_factor=0.5,
               status_forcelist=(429, 500, 502, 503, 504))
# `~` is the v2 API's wildcard path segment; keep it unquoted in URLs.
_CONFIGURATION = Configuration(host=API_BASE, retries=_RETRY,
                               safe_chars_for_path_param="~")
_API_CLIENT = ApiClient(_CONFIGURATION)
_DAG_RUN_API = DagRunApi(_API_CLIENT)
_TASK_INSTANCE_API = TaskInstanceApi(_API_CLIENT)
_DAG_API = DAGApi(_API_CLIENT)
_VARIABLE_API = VariableApi(_API_CLIENT)
_CONNECTION_API = ConnectionApi(_API_CLIENT)


def _login() -> None:
    """Fetch a long-lived JWT and store it on the client configuration."""
    resp = requests.post(LOGIN_PATH, json={"username": USER, "password": PASSWORD},
                         timeout=30)
    # The login route answers 201 with a JSON body; accept any 2xx carrying
    # an access_token.
    token = None
    if resp.status_code < 300:
        token = resp.json().get("access_token")
    if not token:
        raise RuntimeError(
            f"Failed to obtain an Airflow API token via {LOGIN_PATH}: "
            f"{resp.status_code} {resp.text[:200]}")
    _CONFIGURATION.access_token = token


def _call(api_method, *args, **kwargs):
    """Invoke a client method; log in lazily, re-login once on 401/403.

    The api-server answers 401 for a missing/expired JWT and 403 for a JWT
    it cannot validate (e.g. after a jwt_secret rotation); either way the
    cached token is stale, so log in again and retry once.
    """
    kwargs.setdefault("_request_timeout", 30)
    if _CONFIGURATION.access_token is None:
        _login()
    try:
        return api_method(*args, **kwargs)
    except (UnauthorizedException, ForbiddenException):
        _login()
        return api_method(*args, **kwargs)


def _paginate(fetch, list_key: str, limit: int = 100, **params) -> list:
    """Fetch every page of a v2 list endpoint (limit/offset pagination)."""
    offset = 0
    items = []
    while True:
        page = _call(fetch, limit=limit, offset=offset, **params)
        batch = getattr(page, list_key)
        items.extend(batch)
        if len(items) >= (page.total_entries or 0) or len(batch) < limit:
            break
        offset += limit
    return items


def _state_param(state):
    """v2 list endpoints take `state` as a repeated query param (a comma
    separated string is rejected with a 422); the client types it as a
    list, so normalize a single value to a one-item list."""
    if state is None:
        return None
    if isinstance(state, str):
        state = [state]
    return list(state)


def list_dag_runs(dag_id: str = "~", state=None) -> list:
    """Dag runs of one DAG, or of every DAG with ``dag_id="~"``.

    Each item carries dag_id, dag_run_id, state, start_date, end_date, ...
    """
    return _paginate(_DAG_RUN_API.get_dag_runs, "dag_runs", dag_id=dag_id,
                     state=_state_param(state))


def list_task_instances(dag_id: str = "~", dag_run_id: str = "~",
                        state=None, queue=None) -> list:
    """Task instances of one dag run, or of every DAG/run with ``"~"``.

    ``state`` may be a single value or a list (e.g. ``State.unfinished``);
    ``queue`` a string. Each item carries dag_id, dag_run_id, task_id,
    map_index, state, queue.
    """
    if queue is not None and not isinstance(queue, list):
        queue = [queue]
    return _paginate(_TASK_INSTANCE_API.get_task_instances, "task_instances",
                     dag_id=dag_id, dag_run_id=dag_run_id,
                     state=_state_param(state), queue=queue)


def get_dag_run(dag_id: str, dag_run_id: str):
    """Dag run detail (state, logical_date, start_date, end_date, conf, ...)
    or None if the run does not exist."""
    try:
        return _call(_DAG_RUN_API.get_dag_run, dag_id, dag_run_id)
    except NotFoundException:
        return None
    except ApiException as exc:
        raise RuntimeError(f"GET dagRun {dag_id}/{dag_run_id} -> "
                           f"{exc.status} {exc.body[:300] if exc.body else ''}") from exc


def set_task_instance_state(dag_id: str, dag_run_id: str, task_id: str,
                            map_index: int, new_state: str) -> None:
    """Set a task instance's state via the v2 API.

    The v2 body field is ``new_state`` (not ``state``); ``map_index=-1``
    denotes the non-mapped parent instance. The client validates
    ``new_state`` against the v2 enum before sending.
    """
    try:
        _call(_TASK_INSTANCE_API.patch_task_instance,
              dag_id, dag_run_id, task_id,
              PatchTaskInstanceBody(new_state=new_state),
              map_index=map_index)
    except ApiException as exc:
        raise RuntimeError(f"PATCH {dag_id}/{dag_run_id}/{task_id} -> "
                           f"{exc.status} {exc.body[:300] if exc.body else ''}") from exc


def get_dag(dag_id: str):
    """DAG detail (is_paused, last_parsed_time, tags, ...) or None if the
    DAG is unknown (404 — never parsed, or deleted)."""
    try:
        return _call(_DAG_API.get_dag, dag_id)
    except NotFoundException:
        return None
    except ApiException as exc:
        raise RuntimeError(f"GET dag {dag_id} -> "
                           f"{exc.status} {exc.body[:300] if exc.body else ''}") from exc


def set_dag_paused(dag_id: str, is_paused: bool) -> bool:
    """Pause (True) or resume (False) a DAG. Returns False if the DAG is
    unknown (404), True on success."""
    try:
        _call(_DAG_API.patch_dag, dag_id, DAGPatchBody(is_paused=is_paused))
        return True
    except NotFoundException:
        return False
    except ApiException as exc:
        raise RuntimeError(f"PATCH dag {dag_id} -> "
                           f"{exc.status} {exc.body[:300] if exc.body else ''}") from exc


def trigger_dag_run(dag_id: str, conf: dict = None):
    """Trigger a DAG run and return the created run (dag_run_id, state, ...).

    The v2 trigger does not refuse paused DAGs (unlike the UI); an unknown
    dag_id raises (404).
    """
    try:
        return _call(_DAG_RUN_API.trigger_dag_run, dag_id,
                     TriggerDAGRunPostBody(conf=conf))
    except ApiException as exc:
        raise RuntimeError(f"POST dagRun {dag_id} -> "
                           f"{exc.status} {exc.body[:300] if exc.body else ''}") from exc


def set_dag_run_state(dag_id: str, dag_run_id: str, state: str):
    """Force a DAG run's state (PATCH .../dagRuns/{dag_run_id}).

    ``state="success"`` mirrors the v1 ``set_dag_run_state_to_success``:
    the api-server marks the run and its unfinished task instances success.
    The client validates ``state`` against the mutable-states enum
    (queued/success/failed) before sending.
    """
    try:
        return _call(_DAG_RUN_API.patch_dag_run, dag_id, dag_run_id,
                     DAGRunPatchBody(state=state))
    except ApiException as exc:
        raise RuntimeError(f"PATCH dagRun {dag_id}/{dag_run_id} -> "
                           f"{exc.status} {exc.body[:300] if exc.body else ''}") from exc


def get_variable(key: str):
    """Variable value (raw string) or None if the key does not exist (404)."""
    try:
        return _call(_VARIABLE_API.get_variable, key).value
    except NotFoundException:
        return None
    except ApiException as exc:
        raise RuntimeError(f"GET variable {key} -> "
                           f"{exc.status} {exc.body[:300] if exc.body else ''}") from exc


def set_variable(key: str, value: str) -> None:
    """Create a variable, or replace its value if the key exists.

    POST /variables answers 409 for an existing key, so the update path is
    PATCH /variables/{key} with the same body. ``value`` is stored as a raw
    string; JSON (de)serialization is the caller's job.
    """
    body = VariableBody(key=key, value=value)
    try:
        _call(_VARIABLE_API.post_variable, body)
    except ConflictException:
        try:
            _call(_VARIABLE_API.patch_variable, key, body)
        except ApiException as exc:
            raise RuntimeError(f"PATCH variable {key} -> "
                               f"{exc.status} {exc.body[:300] if exc.body else ''}") from exc
    except ApiException as exc:
        raise RuntimeError(f"POST variable {key} -> "
                           f"{exc.status} {exc.body[:300] if exc.body else ''}") from exc


def upsert_connection(conn_id: str, **fields) -> None:
    """Create a connection, or replace it if the id exists.

    ``fields`` maps ``ConnectionBody`` fields (conn_type, host, login,
    password, extra, ...); ``conn_type`` is required by the API model.
    POST /connections answers 409 for an existing id, so the update path is
    PATCH /connections/{id} with the same body (the server merges the
    provided fields into the existing record).
    """
    body = ConnectionBody(connection_id=conn_id, **fields)
    try:
        _call(_CONNECTION_API.post_connection, body)
    except ConflictException:
        try:
            _call(_CONNECTION_API.patch_connection, conn_id, body)
        except ApiException as exc:
            raise RuntimeError(f"PATCH connection {conn_id} -> "
                               f"{exc.status} {exc.body[:300] if exc.body else ''}") from exc
    except ApiException as exc:
        raise RuntimeError(f"POST connection {conn_id} -> "
                           f"{exc.status} {exc.body[:300] if exc.body else ''}") from exc
