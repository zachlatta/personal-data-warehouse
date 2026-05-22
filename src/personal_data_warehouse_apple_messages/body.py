from __future__ import annotations

from dataclasses import dataclass
import hashlib


@dataclass(frozen=True)
class DecodedMessageBody:
    text: str
    source: str
    status: str
    attributed_body_sha256: str
    error: str = ""


def decode_message_body(*, text: str | None, attributed_body: bytes | None) -> DecodedMessageBody:
    attributed_sha = bytes_sha256(attributed_body) if attributed_body else ""
    decoded = ""
    decode_error = ""
    if attributed_body:
        try:
            decoded = decode_attributed_body(attributed_body)
        except Exception as exc:  # The typedstream format is private and may vary by macOS release.
            decode_error = str(exc)
    if decoded:
        return DecodedMessageBody(
            text=decoded,
            source="attributedBody",
            status="ok",
            attributed_body_sha256=attributed_sha,
        )
    if text:
        return DecodedMessageBody(
            text=text,
            source="text",
            status="fallback_text" if decode_error else "ok",
            attributed_body_sha256=attributed_sha,
            error=decode_error,
        )
    if attributed_body:
        return DecodedMessageBody(
            text="",
            source="attributedBody",
            status="error",
            attributed_body_sha256=attributed_sha,
            error=decode_error or "attributedBody did not contain a decodable string",
        )
    return DecodedMessageBody(text="", source="", status="empty", attributed_body_sha256="")


def decode_attributed_body(value: bytes) -> str:
    import typedstream

    archived = typedstream.unarchive_from_data(value)
    decoded = archived_string(archived)
    if decoded is None:
        return ""
    return decoded


def archived_string(value) -> str | None:
    class_name = archived_class_name(value)
    if class_name in {"NSAttributedString", "NSMutableAttributedString"}:
        contents = list(getattr(value, "contents", []) or [])
        if contents:
            return archived_string(typed_value(contents[0]))
    if class_name in {"NSString", "NSMutableString"} and hasattr(value, "value"):
        return str(value.value)
    if hasattr(value, "value"):
        inner = getattr(value, "value")
        if isinstance(inner, str):
            return inner
        extracted = archived_string(inner)
        if extracted is not None:
            return extracted
    if isinstance(value, str):
        return value
    return None


def archived_class_name(value) -> str:
    clazz = getattr(value, "clazz", None)
    name = getattr(clazz, "name", b"")
    if isinstance(name, bytes):
        return name.decode("utf-8", errors="replace")
    return str(name or "")


def typed_value(value):
    return getattr(value, "value", value)


def bytes_sha256(value: bytes | None) -> str:
    return hashlib.sha256(value or b"").hexdigest()
