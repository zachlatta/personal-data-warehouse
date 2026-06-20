"""WhatsApp Web multidevice client runner.

Registers as a linked device via neonize (Python bindings over whatsmeow),
receives live messages + history sync over a persistent connection, and
flushes records through app ingest via WhatsAppBatcher. Runs inside the
Dagster deployment as a bounded job (see defs/whatsapp_client.py) so it never
trips run monitoring; WhatsApp queues messages while no run is active.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
import html
import io
import threading
from typing import Any

from personal_data_warehouse_whatsapp.batcher import WhatsAppBatcher
from personal_data_warehouse_whatsapp.events import (
    chat_payload_from_group_info,
    contact_payload_from_store_contact,
    history_sync_records,
    message_record_from_event,
    participant_payloads_from_group_info,
)
from personal_data_warehouse_whatsapp.state import WhatsAppUploadState


class WhatsAppClientRunner:
    def __init__(
        self,
        *,
        account: str,
        session_path: Path | str,
        ingest_client,
        upload_state: WhatsAppUploadState | None,
        logger,
        run_seconds: int = 10800,
        flush_interval_seconds: int = 60,
        media_bytes_per_flush: int = 512 * 1024 * 1024,
        media_count_per_flush: int = 200,
        pair_phone: str = "",
        client_id: str = "",
        session_snapshot_callback: Callable[[], Any] | None = None,
        qr_output_dir: Path | str | None = None,
        download_history_media: bool = False,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._account = account
        self._session_path = Path(session_path).expanduser()
        self._logger = logger
        self._run_seconds = max(60, run_seconds)
        self._flush_interval_seconds = max(1, flush_interval_seconds)
        self._pair_phone = pair_phone
        self._client_id = client_id
        self._session_snapshot_callback = session_snapshot_callback
        self._qr_output_dir = Path(qr_output_dir).expanduser() if qr_output_dir is not None else None
        self._download_history_media = download_history_media
        self._now = now or (lambda: datetime.now(tz=UTC))
        self._batcher = WhatsAppBatcher(
            account=account,
            ingest_client=ingest_client,
            upload_state=upload_state,
            logger=logger,
            now=self._now,
            media_bytes_per_flush=media_bytes_per_flush,
            media_count_per_flush=media_count_per_flush,
        )
        self._stop_event = threading.Event()
        self._connected_event = threading.Event()
        self._contacts_dumped = False
        self._groups_dumped = False
        self._totals = {
            "messages_received": 0,
            "history_messages_received": 0,
            "chats_received": 0,
            "contacts_received": 0,
            "participants_received": 0,
            "records_selected": 0,
            "records_skipped": 0,
            "batches_uploaded": 0,
            "media_uploaded": 0,
            "media_bytes_uploaded": 0,
        }
        self._totals_lock = threading.Lock()

    def run(self) -> dict[str, int]:
        # neonize loads native libraries (goneonize, libmagic) at import time;
        # import lazily so merely loading this module never requires them.
        from neonize.client import NewClient
        from neonize.events import (
            ConnectedEv,
            EVENT_TO_INT,
            HistorySyncEv,
            LoggedOutEv,
            MessageEv,
            OfflineSyncCompletedEv,
            PairStatusEv,
        )

        self._session_path.parent.mkdir(parents=True, exist_ok=True)
        self._logger.info(
            "Starting WhatsApp client for %s (session %s, window %ss)",
            self._account,
            self._session_path,
            self._run_seconds,
        )
        client = NewClient(str(self._session_path), uuid=self._client_id or None)

        client.event.qr(self._on_qr)
        client.event(ConnectedEv)(self._on_connected)
        client.event(PairStatusEv)(self._on_pair_status)
        client.event(LoggedOutEv)(self._on_logged_out)
        client.event(OfflineSyncCompletedEv)(self._on_offline_sync_completed)
        client.event(MessageEv)(self._on_message)
        client.event(HistorySyncEv)(self._on_history_sync)
        self._register_ignored_events(client, EVENT_TO_INT)

        connector = threading.Thread(target=self._connect_client, args=(client,), name="whatsapp-connector", daemon=True)
        flusher = threading.Thread(target=self._flush_loop, args=(client,), name="whatsapp-flusher", daemon=True)
        connector.start()
        flusher.start()
        pairer = None
        if self._pair_phone:
            pairer = threading.Timer(10.0, self._maybe_pair_phone, args=(client,))
            pairer.daemon = True
            pairer.start()

        try:
            if not self._stop_event.wait(self._run_seconds):
                self._shutdown(client, "run window elapsed")
        except KeyboardInterrupt:
            self._logger.info("WhatsApp client interrupted")
            self._shutdown(client, "keyboard interrupt")
        finally:
            self._stop_event.set()
            if pairer is not None:
                pairer.cancel()
            flusher.join(timeout=self._flush_interval_seconds + 30)
            connector.join(timeout=30)
            self._flush_once()
            self._snapshot_session("shutdown")
        self._logger.info("WhatsApp client summary: %s", self._totals)
        return dict(self._totals)

    # -- event handlers (called from neonize callback threads) ---------------

    def _connect_client(self, client) -> None:
        try:
            client.connect()
        except Exception as exc:  # noqa: BLE001
            if not self._stop_event.is_set():
                self._logger.warning("WhatsApp client connect failed: %s", exc)
        finally:
            self._stop_event.set()

    def _on_qr(self, _client, qr_data: bytes) -> None:
        rendered = render_qr(qr_data)
        if self._qr_output_dir is not None:
            write_qr_artifacts(qr_data, output_dir=self._qr_output_dir)
            self._logger.info("WhatsApp pairing QR updated at %s", self._qr_output_dir / "whatsapp-pairing.html")
        self._logger.info(
            "WhatsApp pairing required. Scan with the phone (Settings > Linked Devices > Link a Device):\n%s\nRaw pairing payload: %s",
            rendered,
            qr_data.decode("utf-8", errors="replace"),
        )

    def _on_connected(self, _client, _event) -> None:
        self._connected_event.set()
        self._logger.info("WhatsApp client connected")
        self._snapshot_session("connected")

    def _on_pair_status(self, _client, event) -> None:
        self._logger.info("WhatsApp pair status: %s", event)
        self._snapshot_session("pair status")

    def _on_logged_out(self, client, event) -> None:
        self._logger.warning("WhatsApp client logged out (reason: %s); stopping", getattr(event, "Reason", ""))
        self._shutdown(client, "logged out")

    def _on_offline_sync_completed(self, _client, event) -> None:
        self._logger.info("WhatsApp offline catch-up completed (%s)", event)

    def _on_message(self, client, event) -> None:
        try:
            record = message_record_from_event(event)
        except Exception as exc:  # noqa: BLE001 - never kill the event loop
            self._logger.warning("Failed to convert WhatsApp message event: %s", exc)
            return
        if record is None:
            return
        self._batcher.add_message(record.payload)
        if record.media is not None:
            self._batcher.add_media(record.media, downloader=self._downloader(client))
        with self._totals_lock:
            self._totals["messages_received"] += 1

    def _on_history_sync(self, client, event) -> None:
        try:
            records = history_sync_records(event.Data)
        except Exception as exc:  # noqa: BLE001
            self._logger.warning("Failed to convert WhatsApp history sync: %s", exc)
            return
        for chat in records.chats:
            self._batcher.add_chat(chat)
        for contact in records.contacts:
            self._batcher.add_contact(contact)
        downloader = self._downloader(client) if self._download_history_media else None
        for message in records.messages:
            self._batcher.add_message(message.payload)
            if message.media is not None:
                self._batcher.add_media(message.media, downloader=downloader)
        with self._totals_lock:
            self._totals["history_messages_received"] += len(records.messages)
            self._totals["chats_received"] += len(records.chats)
            self._totals["contacts_received"] += len(records.contacts)
        self._logger.info(
            "WhatsApp history sync chunk: %s chats, %s messages, %s pushnames",
            len(records.chats),
            len(records.messages),
            len(records.contacts),
        )

    def _register_ignored_events(self, client, event_to_int: dict[type[Any], int]) -> None:
        # neonize dispatches with self.list_func[code], so every possible event
        # needs a handler even if the warehouse does not care about it.
        for event_type, code in event_to_int.items():
            if code not in client.event.list_func:
                client.event(event_type)(self._on_ignored_event)

    def _on_ignored_event(self, _client, _event) -> None:
        return None

    def _downloader(self, client) -> Callable[[Any], bytes | None]:
        def download(e2e_message) -> bytes | None:
            return client.download_any(e2e_message)

        return download

    def _maybe_pair_phone(self, client) -> None:
        try:
            if client.is_logged_in:
                return
            code = client.PairPhone(self._pair_phone, show_push_notification=True)
            self._logger.info("WhatsApp phone pairing code for %s: %s", self._pair_phone, code)
        except Exception as exc:  # noqa: BLE001
            self._logger.warning("WhatsApp phone pairing failed (scan the QR instead): %s", exc)

    # -- flusher --------------------------------------------------------------

    def _flush_loop(self, client) -> None:
        while not self._stop_event.wait(self._flush_interval_seconds):
            if self._connected_event.is_set() and not self._contacts_dumped:
                self._dump_contacts(client)
            if self._connected_event.is_set() and not self._groups_dumped:
                self._dump_groups(client)
            self._flush_once()

    def _flush_once(self) -> None:
        try:
            summary = self._batcher.flush()
        except Exception as exc:  # noqa: BLE001 - records stay queued in state for retry
            self._logger.warning("WhatsApp batch flush failed (will retry): %s", exc)
            return
        with self._totals_lock:
            self._totals["records_selected"] += summary.records_selected
            self._totals["records_skipped"] += summary.records_skipped
            self._totals["batches_uploaded"] += summary.batches_uploaded
            self._totals["media_uploaded"] += summary.media_uploaded
            self._totals["media_bytes_uploaded"] += summary.media_bytes_uploaded
        self._snapshot_session("flush")

    def _dump_contacts(self, client) -> None:
        self._contacts_dumped = True
        try:
            contacts = client.contact.get_all_contacts()
        except Exception as exc:  # noqa: BLE001
            self._logger.warning("WhatsApp contact store dump failed: %s", exc)
            return
        count = 0
        for contact in contacts:
            payload = contact_payload_from_store_contact(contact)
            if payload is not None:
                self._batcher.add_contact(payload)
                count += 1
        with self._totals_lock:
            self._totals["contacts_received"] += count
        self._logger.info("Queued %s WhatsApp contact-store entries", count)
        self._snapshot_session("contact dump")

    def _dump_groups(self, client) -> None:
        # History sync never carries group subjects/members, so query the joined
        # groups once per run window to give every group chat a real name and a
        # participant roster.
        self._groups_dumped = True
        try:
            groups = client.get_joined_groups()
        except Exception as exc:  # noqa: BLE001
            self._logger.warning("WhatsApp joined-groups dump failed: %s", exc)
            self._groups_dumped = False
            return
        chat_count = 0
        participant_count = 0
        for group in groups:
            chat = chat_payload_from_group_info(group)
            if chat is None:
                continue
            self._batcher.add_chat(chat)
            chat_count += 1
            for participant in participant_payloads_from_group_info(group):
                self._batcher.add_chat_participant(participant)
                participant_count += 1
        with self._totals_lock:
            self._totals["chats_received"] += chat_count
            self._totals["participants_received"] += participant_count
        self._logger.info(
            "Queued %s WhatsApp group chats with %s participants", chat_count, participant_count
        )
        self._snapshot_session("group dump")

    def _snapshot_session(self, reason: str) -> None:
        if self._session_snapshot_callback is None:
            return
        try:
            snapshot = self._session_snapshot_callback()
        except Exception as exc:  # noqa: BLE001
            self._logger.warning("WhatsApp session snapshot failed after %s: %s", reason, exc)
            return
        if snapshot is not None:
            size = getattr(snapshot, "database_bytes_size", 0)
            sha = getattr(snapshot, "database_sha256", "")
            self._logger.info("Saved WhatsApp session snapshot after %s (%s bytes, sha256=%s)", reason, size, sha)

    def _shutdown(self, client, reason: str) -> None:
        if self._stop_event.is_set():
            return
        self._logger.info("Stopping WhatsApp client: %s", reason)
        self._stop_event.set()
        try:
            client.disconnect()
        except Exception:  # noqa: BLE001
            pass
        try:
            client.stop()
        except Exception:  # noqa: BLE001
            pass


def render_qr(data: bytes) -> str:
    import qrcode

    buffer = io.StringIO()
    qr = qrcode.QRCode(border=1)
    qr.add_data(data)
    qr.make(fit=True)
    qr.print_ascii(out=buffer, invert=True)
    return buffer.getvalue()


def write_qr_artifacts(data: bytes, *, output_dir: Path) -> None:
    import qrcode

    output_dir.mkdir(parents=True, exist_ok=True)
    png_path = output_dir / "whatsapp-pairing.png"
    text_path = output_dir / "whatsapp-pairing.txt"
    html_path = output_dir / "whatsapp-pairing.html"

    image = qrcode.make(data)
    tmp_png = png_path.with_suffix(".png.tmp")
    image.save(tmp_png)
    tmp_png.replace(png_path)

    payload = data.decode("utf-8", errors="replace")
    text_path.write_text(payload, encoding="utf-8")
    html_path.write_text(
        """<!doctype html>
<meta charset="utf-8">
<title>WhatsApp Pairing QR</title>
<style>
  body { font-family: -apple-system, BlinkMacSystemFont, sans-serif; margin: 32px; }
  img { width: min(72vw, 520px); image-rendering: pixelated; }
  code { word-break: break-all; }
</style>
<h1>WhatsApp Pairing QR</h1>
<p>Scan in WhatsApp: Settings &gt; Linked Devices &gt; Link a Device.</p>
<img id="qr" src="whatsapp-pairing.png" alt="WhatsApp pairing QR">
<p>This page refreshes the QR image every second while the client is running.</p>
<details><summary>Raw pairing payload</summary><code>"""
        + html.escape(payload)
        + """</code></details>
<script>
  const img = document.getElementById("qr");
  setInterval(() => { img.src = "whatsapp-pairing.png?t=" + Date.now(); }, 1000);
</script>
""",
        encoding="utf-8",
    )
