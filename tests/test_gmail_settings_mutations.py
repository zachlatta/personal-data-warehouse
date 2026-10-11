"""Reviewed Gmail filter and label mutations against a fake Gmail service."""

from __future__ import annotations

import json

import httplib2
from google.auth.exceptions import RefreshError
from googleapiclient.errors import HttpError

from personal_data_warehouse import gmail_mutations
from personal_data_warehouse.config import GMAIL_MODIFY_SCOPE, GMAIL_SETTINGS_BASIC_SCOPE, load_settings
from personal_data_warehouse.gmail_mutations import GmailMutationExecutor
from personal_data_warehouse.gmail_settings_mutations import (
    GMAIL_CREATE_FILTER_OPERATION,
    GMAIL_CREATE_LABEL_OPERATION,
    GMAIL_DELETE_FILTER_OPERATION,
    GMAIL_DELETE_LABEL_OPERATION,
    GMAIL_REPLACE_FILTER_OPERATION,
    GMAIL_UPDATE_LABEL_OPERATION,
    gmail_filter_action,
    gmail_filter_criteria,
)


def _http_error(status: int, message: str = "error") -> HttpError:
    body = json.dumps({"error": {"code": status, "message": message}}).encode()
    return HttpError(httplib2.Response({"status": status}), body)


class _Request:
    def __init__(self, fn):
        self._fn = fn

    def execute(self):
        return self._fn()


class FakeGmail:
    """Labels and filters held in memory, with injectable failures."""

    def __init__(self, labels=None, filters=None):
        self.label_data = [dict(label) for label in labels or []]
        self.filter_data = [dict(item) for item in filters or []]
        self.calls: list[tuple[str, object]] = []
        self.fail_next: dict[str, Exception] = {}
        self.duplicate_create_error = False
        self._next = 100

    def _maybe_fail(self, name):
        error = self.fail_next.pop(name, None)
        if error is not None:
            raise error

    def _id(self, prefix):
        self._next += 1
        return f"{prefix}{self._next}"

    def users(self):
        return self

    # labels -------------------------------------------------------------
    def labels(self):  # noqa: D401 - mirrors the Gmail client
        return _Labels(self)

    # settings.filters ---------------------------------------------------
    def settings(self):
        return self

    def filters(self):
        return _Filters(self)


class _Labels:
    def __init__(self, gmail: FakeGmail):
        self.g = gmail

    def list(self, userId):
        return _Request(lambda: {"labels": [dict(label) for label in self.g.label_data]})

    def create(self, userId, body):
        def run():
            self.g.calls.append(("labels.create", body))
            self.g._maybe_fail("labels.create")
            label = {"id": self.g._id("Label_"), "type": "user", **body}
            self.g.label_data.append(label)
            return label

        return _Request(run)

    def patch(self, userId, id, body):
        def run():
            self.g.calls.append(("labels.patch", (id, body)))
            self.g._maybe_fail("labels.patch")
            for label in self.g.label_data:
                if label["id"] == id:
                    label.update(body)
                    return dict(label)
            raise _http_error(404, "Not Found")

        return _Request(run)

    def delete(self, userId, id):
        def run():
            self.g.calls.append(("labels.delete", id))
            self.g._maybe_fail("labels.delete")
            self.g.label_data = [label for label in self.g.label_data if label["id"] != id]
            return ""

        return _Request(run)


class _Filters:
    def __init__(self, gmail: FakeGmail):
        self.g = gmail

    def list(self, userId):
        return _Request(lambda: {"filter": [dict(item) for item in self.g.filter_data]} if self.g.filter_data else {})

    def create(self, userId, body):
        def run():
            self.g.calls.append(("filters.create", body))
            self.g._maybe_fail("filters.create")
            if self.g.duplicate_create_error:
                # A create whose response was lost: Gmail made it, then says so.
                self.g.filter_data.append({"id": self.g._id("ANe1B"), **body})
                raise _http_error(400, "Filter already exists")
            created = {"id": self.g._id("ANe1B"), **body}
            self.g.filter_data.append(created)
            return created

        return _Request(run)

    def delete(self, userId, id):
        def run():
            self.g.calls.append(("filters.delete", id))
            self.g._maybe_fail("filters.delete")
            before = len(self.g.filter_data)
            self.g.filter_data = [item for item in self.g.filter_data if item["id"] != id]
            if len(self.g.filter_data) == before:
                raise _http_error(404, "Not Found")
            return ""

        return _Request(run)


SYSTEM = [{"id": name, "name": name, "type": "system"} for name in ("INBOX", "UNREAD", "STARRED", "IMPORTANT", "SPAM", "TRASH")]
NEWSLETTERS = {"id": "Label_7", "name": "Newsletters", "type": "user"}
OLD_FILTER = {
    "id": "ANe1Bold",
    "criteria": {"from": "news@example.com"},
    "action": {"addLabelIds": ["Label_7"], "removeLabelIds": ["INBOX"]},
}


def _executor(gmail: FakeGmail) -> GmailMutationExecutor:
    return GmailMutationExecutor(settings=None, service_factory=lambda account: gmail)  # type: ignore[arg-type]


def _mutation(operation: str, payload: dict) -> dict:
    return {"id": "mut_1", "provider": "gmail", "operation": operation, "account": "zach@example.test", "payload_json": payload}


# --- translation -------------------------------------------------------------


def test_friendly_actions_map_to_gmail_system_labels() -> None:
    labels = [*SYSTEM, NEWSLETTERS]
    action, created = gmail_filter_action(
        None,
        {
            "add_labels": ["Newsletters"],
            "skip_inbox": True,
            "mark_read": True,
            "star": True,
            "never_spam": True,
            "category": "promotions",
        },
        labels,
    )
    assert created == []
    assert action == {
        "addLabelIds": ["Label_7", "STARRED", "CATEGORY_PROMOTIONS"],
        "removeLabelIds": ["INBOX", "UNREAD", "SPAM"],
    }
    assert gmail_filter_action(None, {"never_important": True, "trash": True}, labels)[0] == {
        "addLabelIds": ["TRASH"],
        "removeLabelIds": ["IMPORTANT"],
    }
    assert gmail_filter_action(None, {"category": "primary"}, labels)[0] == {"addLabelIds": ["CATEGORY_PERSONAL"]}


def test_criteria_translate_to_gmail_names_and_drop_empty_fields() -> None:
    assert gmail_filter_criteria(
        {"from": "a@example.com", "negated_query": "unsubscribe", "has_attachment": True, "exclude_chats": False,
         "size": 1000000, "size_comparison": "larger", "subject": ""}
    ) == {"from": "a@example.com", "negatedQuery": "unsubscribe", "hasAttachment": True, "size": 1000000, "sizeComparison": "larger"}


def test_forwarding_is_refused_even_if_a_payload_carries_it() -> None:
    gmail = FakeGmail(labels=SYSTEM)
    result = _executor(gmail).execute(
        _mutation(GMAIL_CREATE_FILTER_OPERATION, {"criteria": {"from": "a@example.com"}, "action": {"forward": "x@example.com"}})
    )
    assert result.status == "failed_terminal" and "forward" in result.error
    assert gmail.calls == []


# --- create_filter -----------------------------------------------------------


def test_create_filter_creates_the_rule_with_resolved_label_ids() -> None:
    gmail = FakeGmail(labels=[*SYSTEM, NEWSLETTERS])
    result = _executor(gmail).execute(
        _mutation(GMAIL_CREATE_FILTER_OPERATION, {
            "criteria": {"from": "news@example.com"},
            "action": {"add_labels": ["Newsletters"], "skip_inbox": True},
        })
    )
    assert result.status == "succeeded", result.error
    assert result.result_json["filter_id"].startswith("ANe1B")
    assert gmail.calls == [("filters.create", {
        "criteria": {"from": "news@example.com"},
        "action": {"addLabelIds": ["Label_7"], "removeLabelIds": ["INBOX"]},
    })]


def test_create_filter_does_not_duplicate_an_identical_existing_rule() -> None:
    gmail = FakeGmail(labels=[*SYSTEM, NEWSLETTERS], filters=[OLD_FILTER])
    result = _executor(gmail).execute(
        _mutation(GMAIL_CREATE_FILTER_OPERATION, {
            "criteria": {"from": "NEWS@example.com"},
            "action": {"skip_inbox": True, "add_labels": ["newsletters"]},
        })
    )
    assert result.status == "succeeded"
    assert result.result_json == {**result.result_json, "filter_id": "ANe1Bold", "already_existed": True}
    assert [name for name, _ in gmail.calls] == []


def test_create_filter_treats_gmails_already_exists_answer_as_success() -> None:
    gmail = FakeGmail(labels=[*SYSTEM, NEWSLETTERS])
    gmail.duplicate_create_error = True
    result = _executor(gmail).execute(
        _mutation(GMAIL_CREATE_FILTER_OPERATION, {"criteria": {"from": "x@example.com"}, "action": {"star": True}})
    )
    assert result.status == "succeeded", result.error
    assert result.result_json["already_existed"] is True
    assert len(gmail.filter_data) == 1


def test_create_filter_refuses_an_unknown_label_before_creating_anything() -> None:
    gmail = FakeGmail(labels=SYSTEM)
    result = _executor(gmail).execute(
        _mutation(GMAIL_CREATE_FILTER_OPERATION, {
            "criteria": {"from": "x@example.com"},
            "action": {"add_labels": ["Nope"], "create_and_add_labels": ["New"]},
        })
    )
    assert result.status == "failed_terminal" and "Nope" in result.error
    assert gmail.calls == []


def test_create_filter_creates_a_missing_label_only_when_asked() -> None:
    gmail = FakeGmail(labels=SYSTEM)
    result = _executor(gmail).execute(
        _mutation(GMAIL_CREATE_FILTER_OPERATION, {
            "criteria": {"subject": "receipt"},
            "action": {"create_and_add_labels": ["Receipts"]},
        })
    )
    assert result.status == "succeeded", result.error
    created_id = result.result_json["created_labels"][0]["id"]
    assert gmail.calls[0] == ("labels.create", {"name": "Receipts"})
    assert gmail.calls[1][1]["action"] == {"addLabelIds": [created_id]}


# --- delete_filter -----------------------------------------------------------


def test_delete_filter_deletes_the_reviewed_rule() -> None:
    gmail = FakeGmail(labels=SYSTEM, filters=[OLD_FILTER])
    result = _executor(gmail).execute(
        _mutation(GMAIL_DELETE_FILTER_OPERATION, {
            "filter_id": "ANe1Bold",
            "expected_filter": {"criteria": OLD_FILTER["criteria"], "action": OLD_FILTER["action"]},
        })
    )
    assert result.status == "succeeded"
    assert gmail.filter_data == []
    assert result.result_json["deleted_filter"]["criteria"] == {"from": "news@example.com"}


def test_delete_filter_already_gone_is_success_and_says_so() -> None:
    gmail = FakeGmail(labels=SYSTEM)
    result = _executor(gmail).execute(
        _mutation(GMAIL_DELETE_FILTER_OPERATION, {"filter_id": "ANe1Bold", "expected_filter": {"criteria": {}, "action": {}}})
    )
    assert result.status == "succeeded" and result.result_json["already_deleted"] is True
    assert gmail.calls == []


def test_delete_filter_refuses_a_rule_that_changed_since_review() -> None:
    changed = {**OLD_FILTER, "action": {"addLabelIds": ["TRASH"]}}
    gmail = FakeGmail(labels=SYSTEM, filters=[changed])
    result = _executor(gmail).execute(
        _mutation(GMAIL_DELETE_FILTER_OPERATION, {
            "filter_id": "ANe1Bold",
            "expected_filter": {"criteria": OLD_FILTER["criteria"], "action": OLD_FILTER["action"]},
        })
    )
    assert result.status == "failed_terminal" and "changed since" in result.error
    assert gmail.filter_data == [changed]


# --- replace_filter ----------------------------------------------------------


REPLACE_PAYLOAD = {
    "filter_id": "ANe1Bold",
    "expected_filter": {"criteria": OLD_FILTER["criteria"], "action": OLD_FILTER["action"]},
    "criteria": {"from": "news@example.com"},
    "action": {"add_labels": ["Newsletters"], "skip_inbox": True, "mark_read": True},
}


def test_replace_filter_creates_the_new_rule_before_deleting_the_old() -> None:
    gmail = FakeGmail(labels=[*SYSTEM, NEWSLETTERS], filters=[OLD_FILTER])
    result = _executor(gmail).execute(_mutation(GMAIL_REPLACE_FILTER_OPERATION, REPLACE_PAYLOAD))
    assert result.status == "succeeded", result.error
    assert [name for name, _ in gmail.calls] == ["filters.create", "filters.delete"]
    assert [item["id"] for item in gmail.filter_data] == [result.result_json["new_filter_id"]]
    assert gmail.filter_data[0]["action"]["removeLabelIds"] == ["INBOX", "UNREAD"]


def test_replace_filter_keeps_the_old_rule_when_the_create_fails() -> None:
    gmail = FakeGmail(labels=[*SYSTEM, NEWSLETTERS], filters=[OLD_FILTER])
    gmail.fail_next["filters.create"] = _http_error(400, "Invalid filter")
    result = _executor(gmail).execute(_mutation(GMAIL_REPLACE_FILTER_OPERATION, REPLACE_PAYLOAD))
    assert result.status == "failed_terminal" and "Invalid filter" in result.error
    assert gmail.filter_data == [OLD_FILTER]


def test_replace_filter_whose_delete_fails_is_retryable_and_the_retry_creates_nothing_new() -> None:
    gmail = FakeGmail(labels=[*SYSTEM, NEWSLETTERS], filters=[OLD_FILTER])
    gmail.fail_next["filters.delete"] = _http_error(400, "Precondition check failed")
    first = _executor(gmail).execute(_mutation(GMAIL_REPLACE_FILTER_OPERATION, REPLACE_PAYLOAD))
    assert first.status == "failed_retryable"
    new_id = first.result_json["new_filter_id"]
    assert first.result_json["old_filter_deleted"] is False
    assert "new filter" in first.error
    assert {item["id"] for item in gmail.filter_data} == {"ANe1Bold", new_id}

    retry = _executor(gmail).execute(_mutation(GMAIL_REPLACE_FILTER_OPERATION, REPLACE_PAYLOAD))
    assert retry.status == "succeeded", retry.error
    assert retry.result_json["new_filter_id"] == new_id
    assert [item["id"] for item in gmail.filter_data] == [new_id]
    assert [name for name, _ in gmail.calls].count("filters.create") == 1


def test_replace_filter_after_it_already_ran_succeeds_without_writing() -> None:
    gmail = FakeGmail(labels=[*SYSTEM, NEWSLETTERS], filters=[OLD_FILTER])
    _executor(gmail).execute(_mutation(GMAIL_REPLACE_FILTER_OPERATION, REPLACE_PAYLOAD))
    gmail.calls.clear()
    again = _executor(gmail).execute(_mutation(GMAIL_REPLACE_FILTER_OPERATION, REPLACE_PAYLOAD))
    assert again.status == "succeeded" and again.result_json["already_replaced"] is True
    assert gmail.calls == []


def test_replace_filter_whose_old_rule_is_gone_creates_nothing() -> None:
    gmail = FakeGmail(labels=[*SYSTEM, NEWSLETTERS])
    result = _executor(gmail).execute(_mutation(GMAIL_REPLACE_FILTER_OPERATION, REPLACE_PAYLOAD))
    assert result.status == "failed_terminal" and "no longer exists" in result.error
    assert gmail.calls == []


# --- labels ------------------------------------------------------------------


def test_create_label_creates_it_with_color_and_visibility() -> None:
    gmail = FakeGmail(labels=SYSTEM)
    result = _executor(gmail).execute(
        _mutation(GMAIL_CREATE_LABEL_OPERATION, {
            "name": "pdw-test",
            "color": {"background_color": "#16a766", "text_color": "#ffffff"},
            "label_list_visibility": "labelShow",
        })
    )
    assert result.status == "succeeded", result.error
    assert gmail.calls == [("labels.create", {
        "name": "pdw-test",
        "color": {"backgroundColor": "#16a766", "textColor": "#ffffff"},
        "labelListVisibility": "labelShow",
    })]
    assert result.result_json["label_id"].startswith("Label_")


def test_create_label_that_already_exists_records_the_existing_id() -> None:
    gmail = FakeGmail(labels=[*SYSTEM, NEWSLETTERS])
    result = _executor(gmail).execute(_mutation(GMAIL_CREATE_LABEL_OPERATION, {"name": "newsletters"}))
    assert result.status == "succeeded"
    assert result.result_json == {**result.result_json, "label_id": "Label_7", "already_existed": True}
    assert gmail.calls == []


def test_update_label_renames_and_recolors() -> None:
    gmail = FakeGmail(labels=[*SYSTEM, NEWSLETTERS])
    result = _executor(gmail).execute(
        _mutation(GMAIL_UPDATE_LABEL_OPERATION, {
            "label_id": "Label_7", "expected_name": "Newsletters", "new_name": "Reading",
            "color": {"background_color": "#4a86e8", "text_color": "#ffffff"},
        })
    )
    assert result.status == "succeeded", result.error
    assert gmail.calls == [("labels.patch", ("Label_7", {
        "name": "Reading", "color": {"backgroundColor": "#4a86e8", "textColor": "#ffffff"},
    }))]
    assert result.result_json["previous_label"]["name"] == "Newsletters"


def test_update_label_rename_onto_an_existing_name_fails_clearly() -> None:
    gmail = FakeGmail(labels=[*SYSTEM, NEWSLETTERS, {"id": "Label_8", "name": "Reading", "type": "user"}])
    result = _executor(gmail).execute(
        _mutation(GMAIL_UPDATE_LABEL_OPERATION, {"label_id": "Label_7", "expected_name": "Newsletters", "new_name": "reading"})
    )
    assert result.status == "failed_terminal" and "already exists" in result.error
    assert gmail.calls == []


def test_update_label_refuses_a_label_renamed_since_review() -> None:
    gmail = FakeGmail(labels=[*SYSTEM, {"id": "Label_7", "name": "Something else", "type": "user"}])
    result = _executor(gmail).execute(
        _mutation(GMAIL_UPDATE_LABEL_OPERATION, {"label_id": "Label_7", "expected_name": "Newsletters", "new_name": "Reading"})
    )
    assert result.status == "failed_terminal" and "renamed" in result.error


def test_delete_label_deletes_and_is_idempotent() -> None:
    gmail = FakeGmail(labels=[*SYSTEM, NEWSLETTERS])
    payload = {"label_id": "Label_7", "expected_name": "Newsletters"}
    first = _executor(gmail).execute(_mutation(GMAIL_DELETE_LABEL_OPERATION, payload))
    assert first.status == "succeeded" and ("labels.delete", "Label_7") in gmail.calls
    again = _executor(gmail).execute(_mutation(GMAIL_DELETE_LABEL_OPERATION, payload))
    assert again.status == "succeeded" and again.result_json["already_deleted"] is True


def test_delete_label_refuses_system_labels() -> None:
    gmail = FakeGmail(labels=SYSTEM)
    result = _executor(gmail).execute(_mutation(GMAIL_DELETE_LABEL_OPERATION, {"label_id": "INBOX", "expected_name": "INBOX"}))
    assert result.status == "failed_terminal" and "system label" in result.error
    assert gmail.calls == []


# --- credentials -------------------------------------------------------------


def test_filter_changes_request_settings_basic_and_a_missing_grant_is_blocked_with_the_fix(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@example.test")
    settings = load_settings(require_postgres=False, require_gmail=False)
    requested = []

    def fake_load(*, email_address, settings, scopes, service_name, **kwargs):
        requested.append(tuple(scopes))
        raise RefreshError("invalid_scope: Bad Request", {"error": "invalid_scope"})

    monkeypatch.setattr(gmail_mutations, "load_google_credentials", fake_load)
    result = GmailMutationExecutor(settings=settings).execute(
        _mutation(GMAIL_CREATE_FILTER_OPERATION, {"criteria": {"from": "a@example.com"}, "action": {"star": True}})
    )
    assert result.status == "blocked_missing_credentials"
    assert GMAIL_SETTINGS_BASIC_SCOPE in requested[0] and GMAIL_MODIFY_SCOPE in requested[0]
    assert "gmail.settings.basic" in result.error
    assert "--production-scopes" in result.error


def test_label_changes_do_not_request_settings_basic(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@example.test")
    settings = load_settings(require_postgres=False, require_gmail=False)
    requested = []

    def fake_load(*, email_address, settings, scopes, service_name, **kwargs):
        requested.append(tuple(scopes))
        raise RefreshError("invalid_grant")

    monkeypatch.setattr(gmail_mutations, "load_google_credentials", fake_load)
    result = GmailMutationExecutor(settings=settings).execute(_mutation(GMAIL_CREATE_LABEL_OPERATION, {"name": "x"}))
    assert result.status == "blocked_missing_credentials"
    assert requested == [(GMAIL_MODIFY_SCOPE,)]
