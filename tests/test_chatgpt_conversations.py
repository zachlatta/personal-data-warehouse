from __future__ import annotations

from datetime import UTC, datetime

from personal_data_warehouse.agent_sessions_drive_ingest import (
    chatgpt_conversation_to_event_rows,
)

INGESTED_AT = datetime(2026, 6, 22, 18, tzinfo=UTC)
SESSION = "6a1b0e9c-afd8-83ea-9daf-d905e96957ab"


def _node(node_id, parent, children, message):
    return {"id": node_id, "parent": parent, "children": list(children), "message": message}


def _message(msg_id, role, *, parts=None, create_time=1_750_000_000.0, **extra):
    content = extra.pop("content", None)
    if content is None:
        content = {"content_type": "text", "parts": parts if parts is not None else [""]}
    message = {
        "id": msg_id,
        "author": {"role": role, "name": extra.pop("author_name", None), "metadata": {}},
        "create_time": create_time,
        "content": content,
        "metadata": extra.pop("metadata", {}),
        "recipient": extra.pop("recipient", "all"),
    }
    message.update(extra)
    return message


def sample_conversation():
    """A realistic tree: system context -> user -> assistant -> tool call/result.

    The final user turn was edited, producing two sibling children (a branch);
    both branches are retained.
    """
    return {
        "title": "Supergrok rundown",
        "conversation_id": SESSION,
        "create_time": 1_750_000_000.0,
        "update_time": 1_750_000_400.0,
        "gizmo_id": "g-research",
        "current_node": "n-assistant-2b",
        "mapping": {
            "n-root": _node("n-root", None, ["n-sys"], None),
            "n-sys": _node(
                "n-sys",
                "n-root",
                ["n-user-1"],
                _message(
                    "m-sys",
                    "system",
                    create_time=None,
                    content={
                        "content_type": "user_editable_context",
                        "user_profile": "Name is User.",
                        "user_instructions": "Be concise.",
                    },
                    metadata={"is_visually_hidden_from_conversation": True},
                ),
            ),
            "n-user-1": _node(
                "n-user-1",
                "n-sys",
                ["n-assistant-1"],
                _message("m-user-1", "user", parts=["what is supergrok"], create_time=1_750_000_100.0),
            ),
            "n-assistant-1": _node(
                "n-assistant-1",
                "n-user-1",
                ["n-asst-tool"],
                _message(
                    "m-asst-1",
                    "assistant",
                    parts=["Let me look that up."],
                    create_time=1_750_000_110.0,
                    metadata={"model_slug": "gpt-4o"},
                ),
            ),
            "n-asst-tool": _node(
                "n-asst-tool",
                "n-assistant-1",
                ["n-tool-result"],
                _message(
                    "m-asst-tool",
                    "assistant",
                    create_time=1_750_000_120.0,
                    recipient="browser",
                    content={"content_type": "code", "text": "search('supergrok')"},
                    metadata={"model_slug": "gpt-4o"},
                ),
            ),
            "n-tool-result": _node(
                "n-tool-result",
                "n-asst-tool",
                ["n-user-2a", "n-user-2b"],
                _message(
                    "m-tool-result",
                    "tool",
                    create_time=1_750_000_130.0,
                    author_name="browser",
                    content={"content_type": "tether_browsing_display", "result": "SuperGrok is a tier."},
                ),
            ),
            # Edit branch: the user re-asked, producing two sibling user turns.
            "n-user-2a": _node(
                "n-user-2a",
                "n-tool-result",
                ["n-assistant-2a"],
                _message("m-user-2a", "user", parts=["summarize that"], create_time=1_750_000_200.0),
            ),
            "n-assistant-2a": _node(
                "n-assistant-2a",
                "n-user-2a",
                [],
                _message(
                    "m-asst-2a",
                    "assistant",
                    parts=["SuperGrok is xAI's premium tier."],
                    create_time=1_750_000_210.0,
                    metadata={"model_slug": "gpt-4o"},
                ),
            ),
            "n-user-2b": _node(
                "n-user-2b",
                "n-tool-result",
                ["n-assistant-2b"],
                _message("m-user-2b", "user", parts=["give me the price"], create_time=1_750_000_300.0),
            ),
            "n-assistant-2b": _node(
                "n-assistant-2b",
                "n-user-2b",
                [],
                _message(
                    "m-asst-2b",
                    "assistant",
                    create_time=1_750_000_310.0,
                    content={
                        "content_type": "thoughts",
                        "thoughts": [{"summary": "Pricing", "content": "It is $300/mo."}],
                    },
                    metadata={"model_slug": "o3"},
                ),
            ),
        },
    }


def rows():
    return chatgpt_conversation_to_event_rows(
        sample_conversation(), account="user@example.com", device="", ingested_at=INGESTED_AT
    )


def test_skips_messageless_root_and_keeps_every_message_node():
    result = rows()
    # 10 mapping nodes; the root has no message, so 9 rows.
    assert len(result) == 9
    assert all(row["source"] == "chatgpt" for row in result)
    assert all(row["session_id"] == SESSION for row in result)
    assert all(row["account"] == "user@example.com" for row in result)


def test_depth_first_seq_order_follows_children_then_branches():
    result = rows()
    assert [row["event_uuid"] for row in result] == [
        "m-sys",
        "m-user-1",
        "m-asst-1",
        "m-asst-tool",
        "m-tool-result",
        "m-user-2a",
        "m-asst-2a",
        "m-user-2b",
        "m-asst-2b",
    ]
    assert [row["seq"] for row in result] == list(range(len(result)))


def test_roles_and_subtypes():
    by_uuid = {row["event_uuid"]: row for row in rows()}
    assert by_uuid["m-sys"]["role"] == "system"
    assert by_uuid["m-sys"]["subtype"] == "user_editable_context"
    assert by_uuid["m-user-1"]["role"] == "user"
    assert by_uuid["m-user-1"]["subtype"] == "message"
    assert by_uuid["m-asst-1"]["role"] == "assistant"
    assert by_uuid["m-asst-1"]["subtype"] == "message"
    # Assistant turn addressed to a tool is a tool_use.
    assert by_uuid["m-asst-tool"]["role"] == "assistant"
    assert by_uuid["m-asst-tool"]["subtype"] == "tool_use"
    assert by_uuid["m-asst-tool"]["tool_name"] == "browser"
    # Tool author role is a tool_result.
    assert by_uuid["m-tool-result"]["role"] == "tool"
    assert by_uuid["m-tool-result"]["subtype"] == "tool_result"
    assert by_uuid["m-tool-result"]["tool_name"] == "browser"
    # Reasoning content becomes a thinking turn.
    assert by_uuid["m-asst-2b"]["subtype"] == "thinking"


def test_text_extraction_across_content_types():
    by_uuid = {row["event_uuid"]: row for row in rows()}
    assert by_uuid["m-sys"]["text"] == "Name is User.\nBe concise."
    assert by_uuid["m-user-1"]["text"] == "what is supergrok"
    assert by_uuid["m-asst-tool"]["text"] == "search('supergrok')"
    assert by_uuid["m-tool-result"]["text"] == "SuperGrok is a tier."
    assert by_uuid["m-asst-2b"]["text"] == "Pricing\nIt is $300/mo."


def test_header_fields_model_title_parent_and_time():
    by_uuid = {row["event_uuid"]: row for row in rows()}
    assert all(row["session_title"] == "Supergrok rundown" for row in rows())
    assert all(row["entrypoint"] == "g-research" for row in rows())
    assert by_uuid["m-asst-1"]["model"] == "gpt-4o"
    assert by_uuid["m-asst-2b"]["model"] == "o3"
    assert by_uuid["m-user-1"]["parent_uuid"] == "n-sys"
    assert by_uuid["m-user-1"]["occurred_at"] == datetime.fromtimestamp(1_750_000_100.0, tz=UTC)
    # Null create_time falls back to epoch (filtered out of started_at in the view).
    assert by_uuid["m-sys"]["occurred_at"] == datetime.fromtimestamp(0, tz=UTC)


def test_raw_json_is_lossless_node():
    by_uuid = {row["event_uuid"]: row for row in rows()}
    assert '"m-user-1"' in by_uuid["m-user-1"]["raw_json"]
    assert '"children"' in by_uuid["m-user-1"]["raw_json"]


def test_hidden_user_profile_context_is_demoted_to_system():
    # ChatGPT's injected user-profile block: role=user but visually hidden.
    convo = {
        "conversation_id": "c-hidden",
        "title": "Hidden context",
        "mapping": {
            "root": _node("root", None, ["ctx"], None),
            "ctx": _node(
                "ctx",
                "root",
                ["q"],
                _message(
                    "m-ctx",
                    "user",
                    create_time=1.0,
                    content={"content_type": "user_editable_context", "user_profile": "Name is User.", "user_instructions": ""},
                    metadata={"is_visually_hidden_from_conversation": True},
                ),
            ),
            "q": _node("q", "ctx", [], _message("m-q", "user", parts=["What's the weather?"], create_time=2.0)),
        },
    }
    rows = chatgpt_conversation_to_event_rows(convo, account="a", device="", ingested_at=INGESTED_AT)
    by_uuid = {r["event_uuid"]: r for r in rows}
    # The hidden profile context is reclassified as system (not a user turn)...
    assert by_uuid["m-ctx"]["role"] == "system"
    assert by_uuid["m-ctx"]["subtype"] == "user_editable_context"
    # ...so the real question is the only user turn.
    user_turns = [r for r in rows if r["role"] == "user"]
    assert len(user_turns) == 1
    assert user_turns[0]["text"] == "What's the weather?"


def test_empty_mapping_yields_no_rows():
    convo = {"conversation_id": "x", "title": "", "mapping": {}}
    assert chatgpt_conversation_to_event_rows(convo, account="a", device="", ingested_at=INGESTED_AT) == []
