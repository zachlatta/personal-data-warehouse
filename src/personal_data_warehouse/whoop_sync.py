from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
import argparse
import json
import logging
import time
from typing import Any

import requests

from personal_data_warehouse.config import Settings, WhoopConfig, load_settings
from personal_data_warehouse.warehouse import warehouse_from_settings
from personal_data_warehouse.whoop_auth import refresh_whoop_token

EPOCH_UTC = datetime.fromtimestamp(0, tz=UTC)
WHOOP_COLLECTIONS = (
    ("cycles", "/v2/cycle", "insert_whoop_cycles", "cycle"),
    ("recovery", "/v2/recovery", "insert_whoop_recoveries", "recovery"),
    ("sleep", "/v2/activity/sleep", "insert_whoop_sleeps", "sleep"),
    ("workout", "/v2/activity/workout", "insert_whoop_workouts", "workout"),
)


class WhoopApiError(RuntimeError):
    pass


class WhoopAuthError(WhoopApiError):
    pass


class WhoopRateLimitedError(WhoopApiError):
    def __init__(self, *, retry_after: int) -> None:
        super().__init__(f"WHOOP API rate limited; retry after {retry_after}s")
        self.retry_after = retry_after


@dataclass(frozen=True)
class WhoopSyncSummary:
    account: str
    sync_type: str
    records_written: int
    collections: dict[str, int]


def public_whoop_sync_summary(summary: WhoopSyncSummary) -> dict[str, Any]:
    return {
        "account": summary.account,
        "sync_type": summary.sync_type,
        "records_written": summary.records_written,
        "collections": dict(summary.collections),
        # Explicitly document that public summaries never carry OAuth material.
        "has_token": False,
    }


class WhoopApiClient:
    def __init__(
        self,
        *,
        token_json: str,
        client_id: str = "",
        client_secret: str = "",
        base_url: str = "https://api.prod.whoop.com/developer",
        token_url: str = "https://api.prod.whoop.com/oauth/oauth2/token",
        timeout_seconds: int = 30,
        max_rate_limit_sleep_seconds: int = 120,
        max_rate_limit_retries: int = 3,
        session: Any | None = None,
        token_refresher: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
        token_updated: Callable[[dict[str, Any]], None] | None = None,
        refresh_enabled: bool = True,
        sleep: Callable[[int], None] | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._token = _parse_token_json(token_json)
        self._client_id = client_id
        self._client_secret = client_secret
        self._base_url = base_url.rstrip("/")
        self._token_url = token_url
        self._timeout_seconds = timeout_seconds
        self._max_rate_limit_sleep_seconds = max_rate_limit_sleep_seconds
        self._max_rate_limit_retries = max_rate_limit_retries
        self._session = session or requests.Session()
        self._token_refresher = token_refresher
        self._token_updated = token_updated
        self._refresh_enabled = refresh_enabled
        self._sleep = sleep or time.sleep
        self._now = now or (lambda: datetime.now(tz=UTC))

    @classmethod
    def from_config(
        cls,
        config: WhoopConfig,
        *,
        token_updated: Callable[[dict[str, Any]], None] | None = None,
        refresh_enabled: bool = True,
    ) -> "WhoopApiClient":
        return cls(
            token_json=config.token_json,
            client_id=config.client_id,
            client_secret=config.client_secret,
            base_url=config.base_url,
            token_url=config.token_url,
            timeout_seconds=config.request_timeout_seconds,
            max_rate_limit_sleep_seconds=config.max_rate_limit_sleep_seconds,
            token_updated=token_updated,
            refresh_enabled=refresh_enabled,
        )

    def get_json(self, path: str, *, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        if not path.startswith("/"):
            path = "/" + path
        url = self._base_url + path
        auth_attempt = 0
        rate_limit_retries = 0
        force_refresh = False
        while auth_attempt < 2:
            self._ensure_fresh_token(force=force_refresh)
            force_refresh = False
            try:
                response = self._session.request(
                    "GET",
                    url,
                    params={key: value for key, value in (params or {}).items() if value not in (None, "")},
                    headers={"Authorization": f"Bearer {self._access_token}"},
                    timeout=self._timeout_seconds,
                )
            except requests.RequestException as exc:
                raise WhoopApiError(f"WHOOP GET {path} failed with a transport error") from exc
            if response.status_code == 401:
                if auth_attempt == 0 and self._refresh_token:
                    auth_attempt += 1
                    force_refresh = True
                    continue
                raise WhoopAuthError("WHOOP access token was rejected; re-run personal-data-warehouse-whoop-auth if refresh also fails")
            if response.status_code == 429:
                retry_after = _retry_after_seconds(response.headers)
                if (
                    rate_limit_retries >= self._max_rate_limit_retries
                    or retry_after > self._max_rate_limit_sleep_seconds
                ):
                    raise WhoopRateLimitedError(retry_after=retry_after)
                rate_limit_retries += 1
                self._sleep(retry_after)
                continue
            if response.status_code >= 400:
                raise WhoopApiError(f"WHOOP GET {path} failed with HTTP {response.status_code}")
            try:
                payload = response.json()
            except ValueError as exc:
                raise WhoopApiError(f"WHOOP GET {path} returned invalid JSON") from exc
            if not isinstance(payload, dict):
                raise WhoopApiError(f"WHOOP GET {path} returned a non-object JSON payload")
            return payload
        raise WhoopAuthError("WHOOP access token refresh did not produce an accepted token")

    @property
    def _access_token(self) -> str:
        return str(self._token.get("access_token") or "")

    @property
    def _refresh_token(self) -> str:
        return str(self._token.get("refresh_token") or "")

    def _ensure_fresh_token(self, *, force: bool = False) -> None:
        if not force and not self._token_expires_soon():
            return
        if not self._refresh_enabled:
            raise WhoopAuthError(
                "WHOOP validation token is expired or rejected; run the OAuth flow again or run the warehouse sync so rotated tokens can be persisted"
            )
        if not self._refresh_token:
            if force:
                raise WhoopAuthError("WHOOP token has no refresh_token; re-run personal-data-warehouse-whoop-auth")
            return
        if self._token_refresher is not None:
            self._token = self._token_refresher(dict(self._token))
        else:
            self._token = refresh_whoop_token(
                dict(self._token),
                client_id=self._client_id,
                client_secret=self._client_secret,
                token_url=self._token_url,
                timeout=self._timeout_seconds,
            )
        if self._token_updated is not None:
            self._token_updated(dict(self._token))

    def _token_expires_soon(self) -> bool:
        expires_at = self._token.get("expires_at")
        if expires_at in (None, ""):
            return False
        try:
            expires_at_int = int(expires_at)
        except (TypeError, ValueError):
            return False
        return expires_at_int <= int(self._now().timestamp()) + 60


class WhoopSyncRunner:
    def __init__(
        self,
        *,
        settings: Settings,
        warehouse,
        logger,
        client_factory: Callable[[WhoopConfig], Any] | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._settings = settings
        self._warehouse = warehouse
        self._logger = logger
        self._client_factory = client_factory
        self._now = now or (lambda: datetime.now(tz=UTC))

    def sync_all(self) -> list[WhoopSyncSummary]:
        config = self._settings.whoop
        if config is None:
            raise RuntimeError("WHOOP is not configured")
        self._warehouse.ensure_whoop_tables()
        state_by_key = self._warehouse.load_whoop_sync_state()
        account = config.account
        synced_at = self._now()
        stored_token_json = self._warehouse.load_whoop_oauth_token(account=account)
        runtime_token_json = newer_whoop_token_json(
            bootstrap_token_json=config.token_json,
            stored_token_json=stored_token_json,
        )
        if runtime_token_json != stored_token_json:
            self._warehouse.upsert_whoop_oauth_token(
                account=account,
                token_json=runtime_token_json,
                updated_at=synced_at,
            )
        runtime_config = replace(config, token_json=runtime_token_json)
        client = self._build_client(runtime_config, persist_rotated_token=True)
        collections_written: dict[str, int] = {}
        failures: list[str] = []

        for collection, sync_fn in (
            ("profile", self._sync_profile),
            ("body_measurement", self._sync_body_measurement),
        ):
            try:
                collections_written[collection] = sync_fn(
                    account=account,
                    client=client,
                    synced_at=synced_at,
                )
                self._insert_state(
                    account=account,
                    collection=collection,
                    watermark_updated_at=synced_at,
                    last_sync_type="snapshot",
                    status="ok",
                    error="",
                    updated_at=synced_at,
                )
            except Exception as exc:
                self._record_failure(
                    account=account,
                    collection=collection,
                    state=state_by_key.get((account, collection)),
                    error=exc,
                    updated_at=synced_at,
                    config=config,
                )
                failures.append(f"{collection}: {exc}")

        for collection, path, insert_method, row_kind in WHOOP_COLLECTIONS:
            state = state_by_key.get((account, collection))
            try:
                written, sync_type, watermark = self._sync_collection(
                    account=account,
                    client=client,
                    collection=collection,
                    path=path,
                    insert_method=insert_method,
                    row_kind=row_kind,
                    state=state,
                    config=config,
                    synced_at=synced_at,
                )
                collections_written[collection] = written
                self._insert_state(
                    account=account,
                    collection=collection,
                    watermark_updated_at=watermark,
                    last_sync_type=sync_type,
                    status="ok",
                    error="",
                    updated_at=synced_at,
                )
            except Exception as exc:
                self._record_failure(
                    account=account,
                    collection=collection,
                    state=state,
                    error=exc,
                    updated_at=synced_at,
                    config=config,
                )
                failures.append(f"{collection}: {exc}")

        if failures:
            raise RuntimeError("WHOOP sync failed for: " + "; ".join(failures))
        return [
            WhoopSyncSummary(
                account=account,
                sync_type="mixed",
                records_written=sum(collections_written.values()),
                collections=collections_written,
            )
        ]

    def validate_all(self) -> dict[str, Any]:
        config = self._settings.whoop
        if config is None:
            raise RuntimeError("WHOOP is not configured")
        client = self._build_client(config, persist_rotated_token=False)
        profile = client.get_json("/v2/user/profile/basic")
        body = client.get_json("/v2/user/measurement/body")
        collection_endpoints: dict[str, dict[str, Any]] = {}
        for collection, path, _insert_method, _row_kind in WHOOP_COLLECTIONS:
            payload = client.get_json(path, params={"limit": 1})
            records = payload.get("records", [])
            if not isinstance(records, list):
                raise WhoopApiError(f"WHOOP collection {path} returned a non-list records field")
            first_record = records[0] if records and isinstance(records[0], Mapping) else {}
            collection_endpoints[collection] = {
                "status": "ok",
                "record_count": len(records),
                "record_fields": sorted(str(key) for key in first_record),
            }
        return {
            "account_configured": bool(config.account),
            "profile_endpoint": "ok",
            "profile_fields": sorted(str(key) for key in profile),
            "body_measurement_endpoint": "ok",
            "body_measurement_fields": sorted(str(key) for key in body),
            "collection_endpoints": collection_endpoints,
        }

    def _build_client(self, config: WhoopConfig, *, persist_rotated_token: bool):
        if self._client_factory is not None:
            return self._client_factory(config)
        token_updated = None
        if persist_rotated_token:
            token_updated = lambda token: self._warehouse.upsert_whoop_oauth_token(
                account=config.account,
                token_json=json.dumps(token, sort_keys=True, separators=(",", ":")),
                updated_at=self._now(),
            )
        return WhoopApiClient.from_config(
            config,
            token_updated=token_updated,
            refresh_enabled=persist_rotated_token,
        )

    def _sync_profile(self, *, account: str, client, synced_at: datetime) -> int:
        payload = client.get_json("/v2/user/profile/basic")
        self._warehouse.insert_whoop_profiles([profile_to_row(account=account, payload=payload, synced_at=synced_at)])
        return 1

    def _sync_body_measurement(self, *, account: str, client, synced_at: datetime) -> int:
        payload = client.get_json("/v2/user/measurement/body")
        self._warehouse.insert_whoop_body_measurements(
            [body_measurement_to_row(account=account, payload=payload, synced_at=synced_at)]
        )
        return 1

    def _sync_collection(
        self,
        *,
        account: str,
        client,
        collection: str,
        path: str,
        insert_method: str,
        row_kind: str,
        state: Mapping[str, Any] | None,
        config: WhoopConfig,
        synced_at: datetime,
    ) -> tuple[int, str, datetime]:
        sync_type, start = self._collection_window_start(state=state, config=config)
        end = rfc3339_utc(synced_at)
        row_factory = _ROW_FACTORY_BY_KIND[row_kind]
        writer = getattr(self._warehouse, insert_method)
        records_written = 0
        watermark = state_datetime(state, "watermark_updated_at")
        self._logger.info("Starting %s WHOOP %s sync for %s", sync_type, collection, account)
        for records in iter_collection_pages(client, path, limit=config.page_size, start=start, end=end):
            rows = [row_factory(account=account, payload=record, synced_at=synced_at) for record in records]
            writer(rows)
            records_written += len(rows)
            for record in records:
                watermark = max(watermark, parse_rfc3339(record.get("updated_at")))
            self._logger.info("Synced %s WHOOP %s records for %s so far", records_written, collection, account)
        # WHOOP collection `start` filters activity time, not `updated_at`. A
        # successful complete page walk therefore advances the scan watermark to
        # this run's end; the configured overlap catches recent score revisions.
        watermark = max(watermark, synced_at)
        return records_written, sync_type, watermark

    def _collection_window_start(self, *, state: Mapping[str, Any] | None, config: WhoopConfig) -> tuple[str, str]:
        if config.force_full_sync or not state:
            return "full", config.full_sync_start
        basis = state_datetime(state, "watermark_updated_at")
        if basis == EPOCH_UTC:
            basis = state_datetime(state, "updated_at")
        if basis == EPOCH_UTC:
            return "full", config.full_sync_start
        start = basis - timedelta(days=config.incremental_lookback_days)
        return "incremental", rfc3339_utc(start)

    def _insert_state(self, **row) -> None:
        self._warehouse.insert_whoop_sync_state(**row)

    def _record_failure(
        self,
        *,
        account: str,
        collection: str,
        state: Mapping[str, Any] | None,
        error: Exception,
        updated_at: datetime,
        config: WhoopConfig,
    ) -> None:
        self._insert_state(
            account=account,
            collection=collection,
            watermark_updated_at=state_datetime(state, "watermark_updated_at"),
            last_sync_type=str((state or {}).get("last_sync_type", "unknown")),
            status="failed",
            error=truncate_error(_redact_config_secrets(str(error), config)),
            updated_at=updated_at,
        )


def iter_collection_pages(
    client,
    path: str,
    *,
    limit: int,
    start: str = "",
    end: str = "",
) -> Iterator[list[Mapping[str, Any]]]:
    next_token = ""
    seen_tokens: set[str] = set()
    while True:
        params: dict[str, Any] = {"limit": limit}
        if start:
            params["start"] = start
        if end:
            params["end"] = end
        if next_token:
            params["nextToken"] = next_token
        payload = client.get_json(path, params=params)
        records = payload.get("records", [])
        if not isinstance(records, list):
            raise WhoopApiError(f"WHOOP collection {path} returned a non-list records field")
        yield [record for record in records if isinstance(record, Mapping)]
        next_token = str(payload.get("next_token") or "")
        if not next_token:
            break
        if next_token in seen_tokens:
            raise WhoopApiError(f"WHOOP collection {path} repeated a pagination token")
        seen_tokens.add(next_token)


def profile_to_row(*, account: str, payload: Mapping[str, Any], synced_at: datetime) -> dict[str, Any]:
    return {
        "account": account,
        "whoop_user_id": _int(payload.get("user_id")),
        "email": _text(payload.get("email")),
        "first_name": _text(payload.get("first_name")),
        "last_name": _text(payload.get("last_name")),
        "raw_json": dict(payload),
        "synced_at": synced_at,
        "sync_version": sync_version_from_datetime(synced_at),
    }


def body_measurement_to_row(*, account: str, payload: Mapping[str, Any], synced_at: datetime) -> dict[str, Any]:
    return {
        "account": account,
        "height_meter": _float(payload.get("height_meter")),
        "weight_kilogram": _float(payload.get("weight_kilogram")),
        "max_heart_rate": _int(payload.get("max_heart_rate")),
        "raw_json": dict(payload),
        "synced_at": synced_at,
        "sync_version": sync_version_from_datetime(synced_at),
    }


def cycle_to_row(*, account: str, payload: Mapping[str, Any], synced_at: datetime) -> dict[str, Any]:
    score = _mapping(payload.get("score"))
    return {
        "account": account,
        "cycle_id": _text(payload.get("id")),
        "whoop_user_id": _int(payload.get("user_id")),
        "created_at": parse_rfc3339(payload.get("created_at")),
        "updated_at": parse_rfc3339(payload.get("updated_at")),
        "start_at": parse_rfc3339(payload.get("start")),
        "end_at": parse_rfc3339(payload.get("end")),
        "timezone_offset": _text(payload.get("timezone_offset")),
        "score_state": _text(payload.get("score_state")),
        "strain": _float(score.get("strain")),
        "kilojoule": _float(score.get("kilojoule")),
        "average_heart_rate": _int(score.get("average_heart_rate")),
        "max_heart_rate": _int(score.get("max_heart_rate")),
        "score_json": score,
        "raw_json": dict(payload),
        "synced_at": synced_at,
        "sync_version": sync_version_from_datetime(synced_at),
    }


def recovery_to_row(*, account: str, payload: Mapping[str, Any], synced_at: datetime) -> dict[str, Any]:
    score = _mapping(payload.get("score"))
    return {
        "account": account,
        "cycle_id": _text(payload.get("cycle_id")),
        "sleep_id": _text(payload.get("sleep_id")),
        "whoop_user_id": _int(payload.get("user_id")),
        "created_at": parse_rfc3339(payload.get("created_at")),
        "updated_at": parse_rfc3339(payload.get("updated_at")),
        "score_state": _text(payload.get("score_state")),
        "user_calibrating": _bool_int(score.get("user_calibrating")),
        "recovery_score": _int(score.get("recovery_score")),
        "resting_heart_rate": _int(score.get("resting_heart_rate")),
        "hrv_rmssd_milli": _float(score.get("hrv_rmssd_milli")),
        "spo2_percentage": _float(score.get("spo2_percentage")),
        "skin_temp_celsius": _float(score.get("skin_temp_celsius")),
        "score_json": score,
        "raw_json": dict(payload),
        "synced_at": synced_at,
        "sync_version": sync_version_from_datetime(synced_at),
    }


def sleep_to_row(*, account: str, payload: Mapping[str, Any], synced_at: datetime) -> dict[str, Any]:
    score = _mapping(payload.get("score"))
    stage_summary = _mapping(score.get("stage_summary"))
    sleep_needed = _mapping(score.get("sleep_needed"))
    return {
        "account": account,
        "sleep_id": _text(payload.get("id")),
        "cycle_id": _text(payload.get("cycle_id")),
        "v1_id": _int(payload.get("v1_id")),
        "whoop_user_id": _int(payload.get("user_id")),
        "created_at": parse_rfc3339(payload.get("created_at")),
        "updated_at": parse_rfc3339(payload.get("updated_at")),
        "start_at": parse_rfc3339(payload.get("start")),
        "end_at": parse_rfc3339(payload.get("end")),
        "timezone_offset": _text(payload.get("timezone_offset")),
        "nap": _bool_int(payload.get("nap")),
        "score_state": _text(payload.get("score_state")),
        "respiratory_rate": _float(score.get("respiratory_rate")),
        "sleep_performance_percentage": _float(score.get("sleep_performance_percentage")),
        "sleep_consistency_percentage": _float(score.get("sleep_consistency_percentage")),
        "sleep_efficiency_percentage": _float(score.get("sleep_efficiency_percentage")),
        "total_in_bed_time_milli": _int(stage_summary.get("total_in_bed_time_milli")),
        "total_awake_time_milli": _int(stage_summary.get("total_awake_time_milli")),
        "total_no_data_time_milli": _int(stage_summary.get("total_no_data_time_milli")),
        "total_light_sleep_time_milli": _int(stage_summary.get("total_light_sleep_time_milli")),
        "total_slow_wave_sleep_time_milli": _int(stage_summary.get("total_slow_wave_sleep_time_milli")),
        "total_rem_sleep_time_milli": _int(stage_summary.get("total_rem_sleep_time_milli")),
        "sleep_cycle_count": _int(stage_summary.get("sleep_cycle_count")),
        "disturbance_count": _int(stage_summary.get("disturbance_count")),
        "stage_summary_json": stage_summary,
        "sleep_needed_json": sleep_needed,
        "score_json": score,
        "raw_json": dict(payload),
        "synced_at": synced_at,
        "sync_version": sync_version_from_datetime(synced_at),
    }


def workout_to_row(*, account: str, payload: Mapping[str, Any], synced_at: datetime) -> dict[str, Any]:
    score = _mapping(payload.get("score"))
    zone_durations = _mapping(score.get("zone_durations"))
    return {
        "account": account,
        "workout_id": _text(payload.get("id")),
        "v1_id": _int(payload.get("v1_id")),
        "whoop_user_id": _int(payload.get("user_id")),
        "created_at": parse_rfc3339(payload.get("created_at")),
        "updated_at": parse_rfc3339(payload.get("updated_at")),
        "start_at": parse_rfc3339(payload.get("start")),
        "end_at": parse_rfc3339(payload.get("end")),
        "timezone_offset": _text(payload.get("timezone_offset")),
        "sport_name": _text(payload.get("sport_name")),
        "sport_id": _int(payload.get("sport_id")),
        "score_state": _text(payload.get("score_state")),
        "strain": _float(score.get("strain")),
        "average_heart_rate": _int(score.get("average_heart_rate")),
        "max_heart_rate": _int(score.get("max_heart_rate")),
        "kilojoule": _float(score.get("kilojoule")),
        "percent_recorded": _float(score.get("percent_recorded")),
        "distance_meter": _float(score.get("distance_meter")),
        "altitude_gain_meter": _float(score.get("altitude_gain_meter")),
        "altitude_change_meter": _float(score.get("altitude_change_meter")),
        "zone_durations_json": zone_durations,
        "score_json": score,
        "raw_json": dict(payload),
        "synced_at": synced_at,
        "sync_version": sync_version_from_datetime(synced_at),
    }


_ROW_FACTORY_BY_KIND = {
    "cycle": cycle_to_row,
    "recovery": recovery_to_row,
    "sleep": sleep_to_row,
    "workout": workout_to_row,
}


def parse_rfc3339(value: Any) -> datetime:
    if not value:
        return EPOCH_UTC
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    text = str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return EPOCH_UTC
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def state_datetime(state: Mapping[str, Any] | None, key: str) -> datetime:
    return parse_rfc3339((state or {}).get(key))


def rfc3339_utc(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sync_version_from_datetime(value: datetime) -> int:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return int(value.astimezone(UTC).timestamp() * 1_000_000)


def truncate_error(value: str, *, max_chars: int = 4000) -> str:
    return value if len(value) <= max_chars else value[: max_chars - 3] + "..."


def newer_whoop_token_json(*, bootstrap_token_json: str, stored_token_json: str) -> str:
    """Choose the newest OAuth token without letting a stale private row block re-auth.

    WHOOP rotates refresh tokens. Normally the private token has the later expiry
    and wins; after the user re-authorizes, the newly generated env bootstrap has
    the later expiry and replaces the stale private row on the next sync.
    """
    if not stored_token_json:
        return bootstrap_token_json
    if not bootstrap_token_json:
        return stored_token_json
    bootstrap = _parse_token_json(bootstrap_token_json)
    stored = _parse_token_json(stored_token_json)
    try:
        bootstrap_expires_at = int(bootstrap.get("expires_at") or 0)
    except (TypeError, ValueError):
        bootstrap_expires_at = 0
    try:
        stored_expires_at = int(stored.get("expires_at") or 0)
    except (TypeError, ValueError):
        stored_expires_at = 0
    return bootstrap_token_json if bootstrap_expires_at > stored_expires_at else stored_token_json


def _parse_token_json(token_json: str) -> dict[str, Any]:
    if not token_json:
        raise WhoopAuthError("WHOOP_TOKEN_JSON or WHOOP_TOKEN_JSON_B64 must be set")
    try:
        parsed = json.loads(token_json)
    except json.JSONDecodeError as exc:
        raise WhoopAuthError("WHOOP token JSON is invalid") from exc
    if not isinstance(parsed, dict):
        raise WhoopAuthError("WHOOP token JSON must be an object")
    if not parsed.get("access_token") and not parsed.get("refresh_token"):
        raise WhoopAuthError("WHOOP token JSON must include access_token or refresh_token")
    return dict(parsed)


def _retry_after_seconds(headers: Mapping[str, Any]) -> int:
    for name in ("Retry-After", "X-RateLimit-Reset"):
        value = headers.get(name)
        if value in (None, ""):
            continue
        try:
            return max(1, int(value))
        except (TypeError, ValueError):
            continue
    return 60


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _text(value: Any) -> str:
    return "" if value is None else str(value)


def _int(value: Any) -> int:
    if value in (None, ""):
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _float(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _bool_int(value: Any) -> int:
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, str):
        return 1 if value.strip().lower() in {"1", "true", "yes", "y", "on"} else 0
    return 1 if value else 0


def _redact_config_secrets(value: str, config: WhoopConfig) -> str:
    redacted = value
    if config.client_secret:
        redacted = redacted.replace(config.client_secret, "[redacted]")
    try:
        token = json.loads(config.token_json) if config.token_json else {}
    except json.JSONDecodeError:
        token = {}
    if isinstance(token, Mapping):
        for key in ("access_token", "refresh_token", "id_token"):
            secret = str(token.get(key) or "")
            if secret:
                redacted = redacted.replace(secret, "[redacted]")
    return redacted


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync WHOOP data into the personal data warehouse.")
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Make read-only WHOOP profile/body API calls and print a redacted validation summary instead of syncing.",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = load_settings(require_postgres=not args.validate, require_gmail=False, require_whoop=True)
    warehouse = warehouse_from_settings(settings) if not args.validate else None
    runner = WhoopSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=logging.getLogger("personal_data_warehouse.whoop_sync"),
    )
    if args.validate:
        print(json.dumps(runner.validate_all(), sort_keys=True, default=str))
        return
    summaries = runner.sync_all()
    print(json.dumps([public_whoop_sync_summary(summary) for summary in summaries], sort_keys=True, default=str))


if __name__ == "__main__":
    main()
