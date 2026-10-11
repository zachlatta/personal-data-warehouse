"""Reviewed changes to Gmail filters and labels.

The proposal half lives in the Go app (``app/internal/mutations/gmail_settings.go``):
it validates the payload, snapshots the filter or label being changed from
``base_gmail.filters`` / ``base_gmail.labels``, and renders the rule for review.
This half runs after approval, against the live mailbox, and re-checks that
the thing it is about to change is still what the reviewer saw.

Gmail has no filter update: a "replace" creates the new filter first and only
then deletes the old one, so a failure part-way never leaves the mailbox
without the rule. Every operation is safe to retry: creates look for an
existing identical filter or same-name label first, deletes treat "already
gone" as done.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from googleapiclient.errors import HttpError

from personal_data_warehouse.gmail_sync import execute_gmail_request


GMAIL_CREATE_FILTER_OPERATION = "gmail.create_filter"
GMAIL_REPLACE_FILTER_OPERATION = "gmail.replace_filter"
GMAIL_DELETE_FILTER_OPERATION = "gmail.delete_filter"
GMAIL_CREATE_LABEL_OPERATION = "gmail.create_label"
GMAIL_UPDATE_LABEL_OPERATION = "gmail.update_label"
GMAIL_DELETE_LABEL_OPERATION = "gmail.delete_label"

GMAIL_FILTER_OPERATIONS = frozenset(
    {GMAIL_CREATE_FILTER_OPERATION, GMAIL_REPLACE_FILTER_OPERATION, GMAIL_DELETE_FILTER_OPERATION}
)
GMAIL_LABEL_OPERATIONS = frozenset(
    {GMAIL_CREATE_LABEL_OPERATION, GMAIL_UPDATE_LABEL_OPERATION, GMAIL_DELETE_LABEL_OPERATION}
)
GMAIL_SETTINGS_OPERATIONS = GMAIL_FILTER_OPERATIONS | GMAIL_LABEL_OPERATIONS

#: What a person must do when a filter change is blocked on credentials. The
#: grant that predates filter support lacks gmail.settings.basic, and a
#: consent replaces the whole grant, so the fix names the flag that asks for
#: every scope production uses rather than just the missing one.
GMAIL_SETTINGS_REGRANT_HINT = (
    "Changing Gmail filters needs the https://www.googleapis.com/auth/gmail.settings.basic scope, which this "
    "account's stored token does not grant. Re-consent with "
    "`uv run personal-data-warehouse-gmail-auth --email {account} --production-scopes`, set the printed "
    "GOOGLE_{slug}_TOKEN_JSON_B64 and GMAIL_{slug}_TOKEN_JSON_B64 on the Dagster deployment, then set this "
    "mutation back to approved."
)

_CRITERIA_FIELDS = {
    "from": "from",
    "to": "to",
    "subject": "subject",
    "query": "query",
    "negated_query": "negatedQuery",
    "has_attachment": "hasAttachment",
    "exclude_chats": "excludeChats",
    "size": "size",
    "size_comparison": "sizeComparison",
}
_ACTION_KEYS = {
    "add_labels",
    "create_and_add_labels",
    "remove_labels",
    "skip_inbox",
    "mark_read",
    "star",
    "mark_important",
    "never_important",
    "never_spam",
    "trash",
    "category",
}
_CATEGORIES = {
    "primary": "CATEGORY_PERSONAL",
    "social": "CATEGORY_SOCIAL",
    "updates": "CATEGORY_UPDATES",
    "forums": "CATEGORY_FORUMS",
    "promotions": "CATEGORY_PROMOTIONS",
}
_FORWARDING_REFUSED = (
    "forwarding is not supported by gmail filter mutations: it needs a verified forwarding address and the "
    "gmail.settings.sharing scope, and sending Zach's mail elsewhere is a separate decision"
)


class GmailSettingsResult:
    """Mirrors GmailMutationResult without importing it (no import cycle)."""

    def __init__(self, status: str, result_json: dict[str, Any], error: str = "") -> None:
        self.status = status
        self.result_json = result_json
        self.error = error


# --- translation --------------------------------------------------------------------


def gmail_filter_criteria(criteria: Mapping[str, Any]) -> dict[str, Any]:
    """The reviewed (snake_case) criteria as Gmail's Filter.criteria."""
    out: dict[str, Any] = {}
    for key, value in dict(criteria or {}).items():
        if key not in _CRITERIA_FIELDS:
            raise ValueError(f"unknown filter criterion {key!r}")
        if value in (None, "", False, 0):
            continue
        if isinstance(value, str):
            value = value.strip()
            if not value:
                continue
        out[_CRITERIA_FIELDS[key]] = value
    if not out:
        raise ValueError("a filter needs at least one criterion")
    return out


def _validate_action(action: Mapping[str, Any]) -> None:
    if "forward" in action:
        raise ValueError(_FORWARDING_REFUSED)
    unknown = sorted(set(action) - _ACTION_KEYS)
    if unknown:
        raise ValueError(f"unknown filter action {unknown[0]!r}")
    category = str(action.get("category") or "").strip().lower()
    if category and category not in _CATEGORIES:
        raise ValueError(f"unknown filter category {category!r}")


def gmail_filter_action(service, action: Mapping[str, Any], labels: list[dict[str, Any]]) -> tuple[dict[str, Any], list[dict[str, str]]]:
    """The reviewed action as Gmail's Filter.action, resolving label names to ids.

    Strict names are resolved before anything is created, so a typo fails
    without leaving a new label behind.
    """
    from personal_data_warehouse import gmail_mutations as gm

    action = dict(action or {})
    _validate_action(action)
    add_ids = gm._resolve_gmail_label_ids(_names(action.get("add_labels")), labels)
    remove_ids = gm._resolve_gmail_label_ids(_names(action.get("remove_labels")), labels)
    created: list[dict[str, str]] = []
    create_names = gm._casefold_unique_strings(_names(action.get("create_and_add_labels")))
    if create_names:
        created_ids, created = gm._resolve_or_create_gmail_label_ids(service=service, names=create_names, labels=labels)
        add_ids.extend(created_ids)
    if action.get("star"):
        add_ids.append("STARRED")
    if action.get("mark_important"):
        add_ids.append("IMPORTANT")
    if action.get("trash"):
        add_ids.append("TRASH")
    category = str(action.get("category") or "").strip().lower()
    if category:
        add_ids.append(_CATEGORIES[category])
    if action.get("skip_inbox"):
        remove_ids.append("INBOX")
    if action.get("mark_read"):
        remove_ids.append("UNREAD")
    if action.get("never_important"):
        remove_ids.append("IMPORTANT")
    if action.get("never_spam"):
        remove_ids.append("SPAM")
    out: dict[str, Any] = {}
    if add_ids:
        out["addLabelIds"] = gm._unique_strings(add_ids)
    if remove_ids:
        out["removeLabelIds"] = gm._unique_strings(remove_ids)
    overlap = set(out.get("addLabelIds", [])) & set(out.get("removeLabelIds", []))
    if overlap:
        raise ValueError(f"filter would both add and remove {sorted(overlap)[0]}")
    if not out:
        raise ValueError("a filter needs at least one action")
    return out, created


def _names(value: Any) -> list[str]:
    if isinstance(value, str) or not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _filter_key(criteria: Mapping[str, Any], action: Mapping[str, Any]) -> tuple:
    """Two filters do the same thing when this matches: Gmail's own duplicate test is equality too."""
    normalized_criteria = []
    for key, value in sorted(dict(criteria or {}).items()):
        if value in (None, "", False, 0) or (key == "sizeComparison" and value == "unspecified"):
            continue
        normalized_criteria.append((key, value.strip().casefold() if isinstance(value, str) else value))
    action = dict(action or {})
    return (
        tuple(normalized_criteria),
        tuple(sorted(str(v) for v in action.get("addLabelIds") or [])),
        tuple(sorted(str(v) for v in action.get("removeLabelIds") or [])),
        str(action.get("forward") or "").strip().casefold(),
    )


def _filter_summary(gmail_filter: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "id": str(gmail_filter.get("id") or ""),
        "criteria": dict(gmail_filter.get("criteria") or {}),
        "action": dict(gmail_filter.get("action") or {}),
    }


# --- execution ----------------------------------------------------------------------


def execute_gmail_settings_mutation(
    *, operation: str, account: str, payload: Mapping[str, Any], service_provider: Callable[[], Any]
) -> GmailSettingsResult:
    from personal_data_warehouse import gmail_mutations as gm
    from personal_data_warehouse.config import env_slug

    try:
        handler = _HANDLERS[operation]
    except KeyError:
        return GmailSettingsResult("failed_terminal", {}, f"unsupported Gmail settings operation: {operation}")
    try:
        # Validate the reviewed payload before asking for credentials, so a
        # malformed proposal never reads as a credential problem.
        _preflight(operation, payload)
        return handler(service_provider(), dict(payload))
    except _PartialReplace as partial:
        status = gm.gmail_mutation_failure_status(partial.cause)
        if status != "blocked_missing_credentials":
            status = "failed_retryable"
        return GmailSettingsResult(status, partial.result_json, partial.message)
    except Exception as exc:  # noqa: BLE001 - classified like every Gmail mutation
        status = gm.gmail_mutation_failure_status(exc)
        error = str(exc)
        if status == "blocked_missing_credentials" and operation in GMAIL_FILTER_OPERATIONS:
            error = f"{error}. " + GMAIL_SETTINGS_REGRANT_HINT.format(account=account, slug=env_slug(account))
        return GmailSettingsResult(status, {}, error)


def _preflight(operation: str, payload: Mapping[str, Any]) -> None:
    if operation in {GMAIL_CREATE_FILTER_OPERATION, GMAIL_REPLACE_FILTER_OPERATION}:
        gmail_filter_criteria(_mapping(payload.get("criteria")))
        _validate_action(_mapping(payload.get("action")))
    if operation in {GMAIL_REPLACE_FILTER_OPERATION, GMAIL_DELETE_FILTER_OPERATION}:
        if not str(payload.get("filter_id") or "").strip():
            raise ValueError("missing filter_id")
        if not isinstance(payload.get("expected_filter"), Mapping):
            raise ValueError("missing the reviewed snapshot of the filter (expected_filter)")
    if operation in {GMAIL_UPDATE_LABEL_OPERATION, GMAIL_DELETE_LABEL_OPERATION}:
        if not str(payload.get("label_id") or "").strip():
            raise ValueError("missing label_id")
    if operation == GMAIL_CREATE_LABEL_OPERATION and not str(payload.get("name") or "").strip():
        raise ValueError("missing label name")


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list_labels(service) -> list[dict[str, Any]]:
    from personal_data_warehouse.gmail_settings_sync import list_gmail_labels

    return list_gmail_labels(service)


def _list_filters(service) -> list[dict[str, Any]]:
    from personal_data_warehouse.gmail_settings_sync import list_gmail_filters

    return list_gmail_filters(service)


def _create_filter_once(service, criteria: dict[str, Any], action: dict[str, Any], filters: list[dict[str, Any]]) -> tuple[str, bool]:
    """Create the filter unless an identical one exists; return (id, already_existed)."""
    key = _filter_key(criteria, action)
    for existing in filters:
        if _filter_key(existing.get("criteria") or {}, existing.get("action") or {}) == key:
            return str(existing.get("id") or ""), True
    try:
        response = execute_gmail_request(
            lambda: service.users().settings().filters().create(
                userId="me", body={"criteria": criteria, "action": action}
            ).execute()
        )
    except HttpError as exc:
        # A create whose response was lost and is retried, or a race with a
        # second worker: Gmail answers "Filter already exists".
        if getattr(exc.resp, "status", None) != 400 or "exist" not in str(exc).lower():
            raise
        for existing in _list_filters(service):
            if _filter_key(existing.get("criteria") or {}, existing.get("action") or {}) == key:
                return str(existing.get("id") or ""), True
        raise
    return str(_mapping(response).get("id") or ""), False


def _delete_filter(service, filter_id: str) -> bool:
    """Delete; False when it was already gone."""
    try:
        execute_gmail_request(lambda: service.users().settings().filters().delete(userId="me", id=filter_id).execute())
    except HttpError as exc:
        if getattr(exc.resp, "status", None) == 404:
            return False
        raise
    return True


def _create_filter(service, payload: dict[str, Any]) -> GmailSettingsResult:
    criteria = gmail_filter_criteria(_mapping(payload.get("criteria")))
    labels = _list_labels(service)
    action, created_labels = gmail_filter_action(service, _mapping(payload.get("action")), labels)
    filter_id, existed = _create_filter_once(service, criteria, action, _list_filters(service))
    result: dict[str, Any] = {"filter_id": filter_id, "criteria": criteria, "action": action, "created_labels": created_labels}
    if existed:
        result["already_existed"] = True
    return GmailSettingsResult("succeeded", result)


def _snapshot_matches(current: Mapping[str, Any], payload: Mapping[str, Any]) -> bool:
    expected = _mapping(payload.get("expected_filter"))
    return _filter_key(current.get("criteria") or {}, current.get("action") or {}) == _filter_key(
        _mapping(expected.get("criteria")), _mapping(expected.get("action"))
    )


def _changed_since_review(filter_id: str) -> str:
    return (
        f"Gmail filter {filter_id} has changed since it was reviewed; nothing was changed. "
        "Propose again against the filter as it is now (base_gmail.filters)."
    )


def _delete_filter_op(service, payload: dict[str, Any]) -> GmailSettingsResult:
    filter_id = str(payload["filter_id"]).strip()
    current = next((item for item in _list_filters(service) if str(item.get("id")) == filter_id), None)
    if current is None:
        return GmailSettingsResult("succeeded", {"filter_id": filter_id, "already_deleted": True})
    if not _snapshot_matches(current, payload):
        return GmailSettingsResult("failed_terminal", {"filter_id": filter_id, "current_filter": _filter_summary(current)}, _changed_since_review(filter_id))
    deleted = _delete_filter(service, filter_id)
    result: dict[str, Any] = {"filter_id": filter_id, "deleted_filter": _filter_summary(current)}
    if not deleted:
        result["already_deleted"] = True
    return GmailSettingsResult("succeeded", result)


class _PartialReplace(Exception):
    def __init__(self, message: str, cause: Exception, result_json: dict[str, Any]) -> None:
        super().__init__(message)
        self.message = message
        self.cause = cause
        self.result_json = result_json


def _replace_filter(service, payload: dict[str, Any]) -> GmailSettingsResult:
    old_id = str(payload["filter_id"]).strip()
    criteria = gmail_filter_criteria(_mapping(payload.get("criteria")))
    labels = _list_labels(service)
    filters = _list_filters(service)
    old = next((item for item in filters if str(item.get("id")) == old_id), None)
    new_key_holder = None
    # Resolve strictly first (no label creation) to look for the new rule; a
    # create_and_add label that does not exist yet means the rule cannot exist.
    try:
        probe_action, _ = gmail_filter_action(None, {**_mapping(payload.get("action")), "create_and_add_labels": []}, labels)
        create_names = _names(_mapping(payload.get("action")).get("create_and_add_labels"))
        if create_names:
            from personal_data_warehouse import gmail_mutations as gm

            probe_action.setdefault("addLabelIds", []).extend(gm._resolve_gmail_label_ids(create_names, labels))
        key = _filter_key(criteria, probe_action)
        new_key_holder = next(
            (item for item in filters if _filter_key(item.get("criteria") or {}, item.get("action") or {}) == key),
            None,
        )
    except ValueError:
        new_key_holder = None
    if old is not None and not _snapshot_matches(old, payload):
        return GmailSettingsResult("failed_terminal", {"filter_id": old_id, "current_filter": _filter_summary(old)}, _changed_since_review(old_id))
    if old is None:
        if new_key_holder is not None:
            return GmailSettingsResult(
                "succeeded",
                {"replaced_filter_id": old_id, "new_filter_id": str(new_key_holder.get("id") or ""), "already_replaced": True},
            )
        return GmailSettingsResult(
            "failed_terminal",
            {"replaced_filter_id": old_id},
            f"Gmail filter {old_id} no longer exists, so there is nothing to replace; no filter was created. "
            "Propose gmail.create_filter if the new rule is still wanted.",
        )
    if new_key_holder is not None and str(new_key_holder.get("id")) == old_id:
        return GmailSettingsResult("succeeded", {"replaced_filter_id": old_id, "new_filter_id": old_id, "already_replaced": True})

    action, created_labels = gmail_filter_action(service, _mapping(payload.get("action")), labels)
    new_id, existed = _create_filter_once(service, criteria, action, filters)
    progress = {
        "replaced_filter_id": old_id,
        "new_filter_id": new_id,
        "created_new_filter": not existed,
        "created_labels": created_labels,
        "previous_filter": _filter_summary(old),
    }
    try:
        _delete_filter(service, old_id)
    except Exception as exc:  # noqa: BLE001 - the new rule is in place; a retry finishes the swap
        raise _PartialReplace(
            f"created the new filter {new_id} but deleting the old filter {old_id} failed: {exc}. "
            "Both rules are in Gmail now; a retry deletes the old one and does not create another.",
            exc,
            {**progress, "old_filter_deleted": False},
        ) from exc
    return GmailSettingsResult("succeeded", {**progress, "old_filter_deleted": True, "criteria": criteria, "action": action})


def _label_body(payload: Mapping[str, Any], *, name_key: str) -> dict[str, Any]:
    body: dict[str, Any] = {}
    name = str(payload.get(name_key) or "").strip()
    if name:
        body["name"] = name
    color = _mapping(payload.get("color"))
    if color:
        body["color"] = {
            "backgroundColor": str(color.get("background_color") or ""),
            "textColor": str(color.get("text_color") or ""),
        }
    for source, target in (("label_list_visibility", "labelListVisibility"), ("message_list_visibility", "messageListVisibility")):
        value = str(payload.get(source) or "").strip()
        if value:
            body[target] = value
    return body


def _same_name(labels: list[dict[str, Any]], name: str) -> list[dict[str, Any]]:
    return [label for label in labels if str(label.get("name") or "").casefold() == name.casefold()]


def _create_label(service, payload: dict[str, Any]) -> GmailSettingsResult:
    from personal_data_warehouse import gmail_mutations as gm

    body = _label_body(payload, name_key="name")
    name = body["name"]
    existing = _same_name(_list_labels(service), name)
    if existing:
        return GmailSettingsResult("succeeded", {"label_id": str(existing[0].get("id") or ""), "name": existing[0].get("name"), "already_existed": True})
    try:
        response = execute_gmail_request(lambda: service.users().labels().create(userId="me", body=body).execute())
    except HttpError as exc:
        if getattr(exc.resp, "status", None) != 409:
            raise
        label_id = gm._resolve_gmail_label_name_id(name, _list_labels(service), required=True)
        return GmailSettingsResult("succeeded", {"label_id": label_id, "name": name, "already_existed": True})
    return GmailSettingsResult("succeeded", {"label_id": str(_mapping(response).get("id") or ""), "name": name, "label": _mapping(response)})


def _update_label(service, payload: dict[str, Any]) -> GmailSettingsResult:
    label_id = str(payload["label_id"]).strip()
    expected_name = str(payload.get("expected_name") or "")
    labels = _list_labels(service)
    current = next((label for label in labels if str(label.get("id")) == label_id), None)
    if current is None:
        return GmailSettingsResult("failed_terminal", {"label_id": label_id}, f"Gmail label {expected_name or label_id} ({label_id}) no longer exists; nothing was changed")
    if current.get("type") == "system":
        return GmailSettingsResult("failed_terminal", {"label_id": label_id}, f"{label_id} is a system label; Gmail does not allow changing it")
    body = _label_body(payload, name_key="new_name")
    current_name = str(current.get("name") or "")
    if expected_name and current_name != expected_name and current_name != body.get("name"):
        return GmailSettingsResult(
            "failed_terminal",
            {"label_id": label_id, "current_name": current_name},
            f"Gmail label {label_id} was renamed from {expected_name!r} to {current_name!r} since it was reviewed; nothing was changed",
        )
    new_name = body.get("name")
    if new_name and new_name != current_name:
        clash = [label for label in _same_name(labels, new_name) if str(label.get("id")) != label_id]
        if clash:
            return GmailSettingsResult(
                "failed_terminal",
                {"label_id": label_id},
                f"cannot rename {current_name!r} to {new_name!r}: a label named {clash[0].get('name')!r} already exists ({clash[0].get('id')})",
            )
    current_color = _mapping(current.get("color"))
    changes = {
        key: value
        for key, value in body.items()
        if (current_color != value if key == "color" else current.get(key) != value)
    }
    if not changes:
        return GmailSettingsResult("succeeded", {"label_id": label_id, "already_applied": True})
    response = execute_gmail_request(lambda: service.users().labels().patch(userId="me", id=label_id, body=changes).execute())
    return GmailSettingsResult("succeeded", {"label_id": label_id, "previous_label": current, "label": _mapping(response)})


def _delete_label(service, payload: dict[str, Any]) -> GmailSettingsResult:
    label_id = str(payload["label_id"]).strip()
    expected_name = str(payload.get("expected_name") or "")
    current = next((label for label in _list_labels(service) if str(label.get("id")) == label_id), None)
    if current is None:
        return GmailSettingsResult("succeeded", {"label_id": label_id, "already_deleted": True})
    if current.get("type") == "system":
        return GmailSettingsResult("failed_terminal", {"label_id": label_id}, f"{label_id} is a system label; Gmail does not allow deleting it")
    if expected_name and str(current.get("name") or "") != expected_name:
        return GmailSettingsResult(
            "failed_terminal",
            {"label_id": label_id, "current_name": current.get("name")},
            f"Gmail label {label_id} is now named {current.get('name')!r}, not {expected_name!r} as reviewed; nothing was deleted",
        )
    try:
        execute_gmail_request(lambda: service.users().labels().delete(userId="me", id=label_id).execute())
    except HttpError as exc:
        if getattr(exc.resp, "status", None) != 404:
            raise
        return GmailSettingsResult("succeeded", {"label_id": label_id, "already_deleted": True})
    return GmailSettingsResult("succeeded", {"label_id": label_id, "deleted_label": current})


_HANDLERS: dict[str, Callable[[Any, dict[str, Any]], GmailSettingsResult]] = {
    GMAIL_CREATE_FILTER_OPERATION: _create_filter,
    GMAIL_REPLACE_FILTER_OPERATION: _replace_filter,
    GMAIL_DELETE_FILTER_OPERATION: _delete_filter_op,
    GMAIL_CREATE_LABEL_OPERATION: _create_label,
    GMAIL_UPDATE_LABEL_OPERATION: _update_label,
    GMAIL_DELETE_LABEL_OPERATION: _delete_label,
}
