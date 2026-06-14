"""Convert neonize/whatsmeow protobuf events into warehouse record payloads.

Everything in this module is pure: it takes protobuf objects and returns
JSON-serializable dicts matching the whatsapp_* warehouse tables. The
network-facing client wiring lives in client.py.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

EPOCH = datetime.fromtimestamp(0, tz=UTC)

# waE2E.Message wrapper fields whose payload is the nested .message.
_WRAPPER_FIELDS = (
    "deviceSentMessage",
    "ephemeralMessage",
    "viewOnceMessage",
    "viewOnceMessageV2",
    "viewOnceMessageV2Extension",
    "documentWithCaptionMessage",
    "editedMessage",
)

# (field name on waE2E.Message, message_type, is media)
_CONTENT_FIELDS = (
    ("conversation", "text", False),
    ("extendedTextMessage", "text", False),
    ("imageMessage", "image", True),
    ("videoMessage", "video", True),
    ("ptvMessage", "video", True),
    ("audioMessage", "audio", True),
    ("documentMessage", "document", True),
    ("stickerMessage", "sticker", True),
    ("contactMessage", "contact", False),
    ("contactsArrayMessage", "contact", False),
    ("locationMessage", "location", False),
    ("liveLocationMessage", "live_location", False),
    ("reactionMessage", "reaction", False),
    ("pollCreationMessage", "poll", False),
    ("pollCreationMessageV2", "poll", False),
    ("groupInviteMessage", "group_invite", False),
    ("eventMessage", "event", False),
    ("protocolMessage", "protocol", False),
)


@dataclass(frozen=True)
class MessageRecord:
    payload: dict[str, Any]
    media: MediaRecord | None = None


@dataclass(frozen=True)
class MediaRecord:
    payload: dict[str, Any]
    # The unwrapped waE2E.Message carrying the media payload; client.py uses
    # it with download_any() to fetch and store the bytes.
    e2e_message: Any = field(compare=False, default=None)


@dataclass(frozen=True)
class HistorySyncRecords:
    chats: tuple[dict[str, Any], ...]
    contacts: tuple[dict[str, Any], ...]
    messages: tuple[MessageRecord, ...]


def jid_to_string(jid) -> str:
    """Render a neonize JID proto as user@server; empty for unset JIDs."""
    if jid is None:
        return ""
    user = str(getattr(jid, "User", "") or "")
    server = str(getattr(jid, "Server", "") or "")
    if not user and not server:
        return ""
    return f"{user}@{server}" if server else user


def normalize_jid_string(value: str) -> str:
    # Strip device/agent suffixes like 1555:12@s.whatsapp.net.
    if "@" not in value:
        return value
    user, _, server = value.partition("@")
    user = user.split(":", 1)[0]
    return f"{user}@{server}"


def chat_type_for_jid(chat_jid: str) -> str:
    server = chat_jid.rpartition("@")[2]
    return {
        "s.whatsapp.net": "user",
        "g.us": "group",
        "broadcast": "broadcast",
        "newsletter": "newsletter",
        "lid": "user",
    }.get(server, server or "unknown")


def timestamp_to_datetime(value: int | float | None) -> datetime:
    try:
        number = float(value or 0)
    except (TypeError, ValueError):
        number = 0
    if number <= 0:
        return EPOCH
    if number > 10_000_000_000:  # milliseconds
        number /= 1000
    return datetime.fromtimestamp(number, tz=UTC)


def unwrap_message(message):
    """Follow wrapper fields (device-sent, ephemeral, view-once, ...) to the payload."""
    current = message
    for _ in range(8):
        if current is None:
            return None
        for wrapper in _WRAPPER_FIELDS:
            if _has_field(current, wrapper):
                nested = getattr(getattr(current, wrapper), "message", None)
                if nested is not None:
                    current = nested
                    break
        else:
            return current
    return current


def classify_message(message) -> tuple[str, str, Any]:
    """Return (message_type, media_type, media_submessage|None) for an unwrapped message."""
    if message is None:
        return "unknown", "", None
    for field_name, message_type, is_media in _CONTENT_FIELDS:
        if field_name == "conversation":
            if str(getattr(message, "conversation", "") or ""):
                return message_type, "", None
            continue
        if _has_field(message, field_name):
            submessage = getattr(message, field_name)
            if message_type == "audio" and bool(getattr(submessage, "PTT", False)):
                return "voice", "voice", submessage
            return message_type, (field_name.removesuffix("Message") if is_media else ""), (
                submessage if is_media else None
            )
    # Group messages carry key-distribution payloads alongside (or without)
    # content; they are transport plumbing, not content.
    plumbing = {
        "messageContextInfo",
        "senderKeyDistributionMessage",
        "fastRatchetKeySenderKeyDistributionMessage",
    }
    for descriptor_field in message.DESCRIPTOR.fields:
        if descriptor_field.name in plumbing:
            continue
        if _has_field(message, descriptor_field.name):
            return descriptor_field.name, "", None
    return "unknown", "", None


def extract_body(message) -> str:
    if message is None:
        return ""
    conversation = str(getattr(message, "conversation", "") or "")
    if conversation:
        return conversation
    if _has_field(message, "extendedTextMessage"):
        return str(message.extendedTextMessage.text or "")
    for media_field in ("imageMessage", "videoMessage", "documentMessage"):
        if _has_field(message, media_field):
            return str(getattr(message, media_field).caption or "")
    if _has_field(message, "contactMessage"):
        return str(message.contactMessage.displayName or "")
    if _has_field(message, "contactsArrayMessage"):
        return str(message.contactsArrayMessage.displayName or "")
    if _has_field(message, "locationMessage"):
        location = message.locationMessage
        return " - ".join(part for part in (str(location.name or ""), str(location.address or "")) if part)
    if _has_field(message, "reactionMessage"):
        return str(message.reactionMessage.text or "")
    if _has_field(message, "pollCreationMessage"):
        return str(message.pollCreationMessage.name or "")
    if _has_field(message, "pollCreationMessageV2"):
        return str(message.pollCreationMessageV2.name or "")
    if _has_field(message, "groupInviteMessage"):
        return str(message.groupInviteMessage.groupName or "")
    if _has_field(message, "eventMessage"):
        return str(getattr(message.eventMessage, "name", "") or "")
    return ""


def extract_quoted_message_id(message) -> str:
    if message is None:
        return ""
    if _has_field(message, "reactionMessage"):
        return str(message.reactionMessage.key.ID or "")
    for descriptor_field in message.DESCRIPTOR.fields:
        if not _has_field(message, descriptor_field.name):
            continue
        submessage = getattr(message, descriptor_field.name)
        context = getattr(submessage, "contextInfo", None)
        if context is not None and str(getattr(context, "stanzaID", "") or ""):
            return str(context.stanzaID)
    return ""


def message_record_from_event(event) -> MessageRecord | None:
    """Convert a neonize MessageEv proto into a message record (live messages)."""
    info = event.Info
    chat_id = normalize_jid_string(jid_to_string(info.MessageSource.Chat))
    sender_jid = normalize_jid_string(jid_to_string(info.MessageSource.Sender))
    message = unwrap_message(event.Message)
    return _build_message_record(
        chat_id=chat_id,
        message_id=str(info.ID or ""),
        sender_jid=sender_jid,
        push_name=str(info.Pushname or ""),
        is_from_me=bool(info.MessageSource.IsFromMe),
        message=message,
        message_at=timestamp_to_datetime(info.Timestamp),
        raw={
            "source": "live",
            "type": str(info.Type or ""),
            "media_type": str(info.MediaType or ""),
            "server_id": int(info.ServerID or 0),
            "is_ephemeral": bool(getattr(event, "IsEphemeral", False)),
            "is_view_once": bool(getattr(event, "IsViewOnce", False)),
            "is_edit": bool(getattr(event, "IsEdit", False)),
        },
    )


def message_record_from_web_message(web_message, *, chat_id: str) -> MessageRecord | None:
    """Convert a waWeb.WebMessageInfo (history sync) into a message record."""
    key = web_message.key
    record_chat_id = normalize_jid_string(str(key.remoteJID or "") or chat_id)
    is_from_me = bool(key.fromMe)
    participant = normalize_jid_string(str(key.participant or "") or str(web_message.participant or ""))
    sender_jid = "" if is_from_me else (participant or record_chat_id)
    message = unwrap_message(web_message.message if web_message.HasField("message") else None)
    return _build_message_record(
        chat_id=record_chat_id,
        message_id=str(key.ID or ""),
        sender_jid=sender_jid,
        push_name=str(web_message.pushName or ""),
        is_from_me=is_from_me,
        message=message,
        message_at=timestamp_to_datetime(web_message.messageTimestamp),
        raw={
            "source": "history_sync",
            "status": int(web_message.status or 0),
            "stub_type": int(web_message.messageStubType or 0),
        },
    )


def _build_message_record(
    *,
    chat_id: str,
    message_id: str,
    sender_jid: str,
    push_name: str,
    is_from_me: bool,
    message,
    message_at: datetime,
    raw: dict[str, Any],
) -> MessageRecord | None:
    if not chat_id or not message_id:
        return None

    # Protocol messages mutate other messages (revoke/edit) instead of being
    # content themselves.
    if message is not None and _has_field(message, "protocolMessage"):
        protocol = message.protocolMessage
        target_id = str(protocol.key.ID or "")
        type_name = protocol.Type.Name(protocol.type) if protocol.type is not None else ""
        if type_name == "REVOKE" and target_id:
            return MessageRecord(
                payload=_message_payload(
                    chat_id=chat_id,
                    message_id=target_id,
                    sender_jid=sender_jid,
                    push_name=push_name,
                    is_from_me=is_from_me,
                    body_text="",
                    message_type="revoke",
                    media_type="",
                    quoted_message_id="",
                    message_at=message_at,
                    edited_at=EPOCH,
                    is_deleted=True,
                    raw={**raw, "revoked_by": message_id},
                )
            )
        if type_name == "MESSAGE_EDIT" and target_id:
            edited = unwrap_message(protocol.editedMessage if protocol.HasField("editedMessage") else None)
            edited_type, edited_media_type, _ = classify_message(edited)
            return MessageRecord(
                payload=_message_payload(
                    chat_id=chat_id,
                    message_id=target_id,
                    sender_jid=sender_jid,
                    push_name=push_name,
                    is_from_me=is_from_me,
                    body_text=extract_body(edited),
                    message_type=edited_type,
                    media_type=edited_media_type,
                    quoted_message_id=extract_quoted_message_id(edited),
                    message_at=message_at,
                    edited_at=message_at,
                    is_deleted=False,
                    raw={**raw, "edited_by": message_id},
                )
            )
        return None

    message_type, media_type, media_submessage = classify_message(message)
    payload = _message_payload(
        chat_id=chat_id,
        message_id=message_id,
        sender_jid=sender_jid,
        push_name=push_name,
        is_from_me=is_from_me,
        body_text=extract_body(message),
        message_type=message_type,
        media_type=media_type,
        quoted_message_id=extract_quoted_message_id(message),
        message_at=message_at,
        edited_at=EPOCH,
        is_deleted=False,
        raw=raw,
    )

    media = None
    if media_submessage is not None:
        media = MediaRecord(
            payload={
                "chat_id": chat_id,
                "message_id": message_id,
                "media_type": media_type,
                "filename": str(getattr(media_submessage, "fileName", "") or ""),
                "mime_type": str(getattr(media_submessage, "mimetype", "") or ""),
                "total_bytes": int(getattr(media_submessage, "fileLength", 0) or 0),
                "size_bytes": 0,
                "file_sha256": bytes(getattr(media_submessage, "fileSHA256", b"") or b"").hex(),
                "content_sha256": "",
                "is_missing": True,
                "error": "media not downloaded",
                "message_at": message_at.isoformat(),
                "raw": {"source": raw.get("source", "")},
            },
            e2e_message=message,
        )
    return MessageRecord(payload=payload, media=media)


def _message_payload(
    *,
    chat_id: str,
    message_id: str,
    sender_jid: str,
    push_name: str,
    is_from_me: bool,
    body_text: str,
    message_type: str,
    media_type: str,
    quoted_message_id: str,
    message_at: datetime,
    edited_at: datetime,
    is_deleted: bool,
    raw: dict[str, Any],
) -> dict[str, Any]:
    return {
        "chat_id": chat_id,
        "message_id": message_id,
        "sender_jid": sender_jid,
        "push_name": push_name,
        "is_from_me": is_from_me,
        "body_text": body_text,
        "message_kind": message_type,
        "media_type": media_type,
        "quoted_message_id": quoted_message_id,
        "message_at": message_at.isoformat(),
        "edited_at": edited_at.isoformat(),
        "is_deleted": is_deleted,
        "raw": raw,
    }


def chat_payload_from_conversation(conversation) -> dict[str, Any] | None:
    chat_id = normalize_jid_string(str(conversation.ID or ""))
    if not chat_id:
        return None
    return {
        "chat_id": chat_id,
        "name": str(getattr(conversation, "name", "") or getattr(conversation, "displayName", "") or ""),
        "chat_type": chat_type_for_jid(chat_id),
        "is_archived": bool(getattr(conversation, "archived", False)),
        "last_message_at": timestamp_to_datetime(
            getattr(conversation, "conversationTimestamp", 0) or getattr(conversation, "lastMsgTimestamp", 0)
        ).isoformat(),
        "raw": {
            "unread_count": int(getattr(conversation, "unreadCount", 0) or 0),
            "pinned": int(getattr(conversation, "pinned", 0) or 0),
            "read_only": bool(getattr(conversation, "readOnly", False)),
        },
    }


def contact_payload_from_pushname(pushname) -> dict[str, Any] | None:
    jid = normalize_jid_string(str(pushname.ID or ""))
    if not jid:
        return None
    return {
        "jid": jid,
        "push_name": str(pushname.pushname or ""),
        "first_name": "",
        "full_name": "",
        "business_name": "",
        "raw": {"source": "history_sync"},
    }


def contact_payload_from_store_contact(contact) -> dict[str, Any] | None:
    jid = normalize_jid_string(jid_to_string(contact.JID))
    if not jid:
        return None
    info = contact.Info
    return {
        "jid": jid,
        "push_name": str(info.PushName or ""),
        "first_name": str(info.FirstName or ""),
        "full_name": str(info.FullName or ""),
        "business_name": str(info.BusinessName or ""),
        "raw": {"source": "contact_store", "found": bool(info.Found)},
    }


def history_sync_records(history) -> HistorySyncRecords:
    """Convert a waHistorySync.HistorySync proto into warehouse records."""
    chats: list[dict[str, Any]] = []
    contacts: list[dict[str, Any]] = []
    messages: list[MessageRecord] = []
    for conversation in history.conversations:
        chat = chat_payload_from_conversation(conversation)
        if chat is None:
            continue
        chats.append(chat)
        for history_message in conversation.messages:
            if not history_message.HasField("message"):
                continue
            record = message_record_from_web_message(history_message.message, chat_id=chat["chat_id"])
            if record is not None:
                messages.append(record)
    for pushname in history.pushnames:
        contact = contact_payload_from_pushname(pushname)
        if contact is not None:
            contacts.append(contact)
    return HistorySyncRecords(chats=tuple(chats), contacts=tuple(contacts), messages=tuple(messages))


def _has_field(message, field_name: str) -> bool:
    try:
        return message.HasField(field_name)
    except ValueError:
        # Scalar/repeated fields (e.g. conversation) do not support HasField.
        return bool(getattr(message, field_name, None))
