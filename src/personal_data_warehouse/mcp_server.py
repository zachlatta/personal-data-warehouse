from __future__ import annotations

import os
from typing import Any

from mcp.server.fastmcp import FastMCP

from personal_data_warehouse.config import load_settings
from personal_data_warehouse.mutation_approval_ui import (
    RunningMutationApprovalUi,
    mutation_approval_ui_config_from_env,
    start_mutation_approval_ui,
)
from personal_data_warehouse.warehouse import warehouse_from_settings


def create_mcp_server(*, approval_ui: RunningMutationApprovalUi) -> FastMCP:
    mcp = FastMCP("personal-data-warehouse")

    @mcp.tool()
    def propose_mutation_request(
        title: str,
        reason: str,
        mutations: list[dict[str, Any]],
        context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Propose a task-level request containing one or more concrete mutations."""
        return propose_mutation_request_batch(
            title=title,
            reason=reason,
            mutations=mutations,
            context=context,
            approval_ui=approval_ui,
        )

    @mcp.tool()
    def propose_gmail_archive_threads(
        account: str,
        thread_ids: list[str],
        reason: str,
        title: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Propose archiving Gmail threads, returning a human approval URL."""
        return propose_gmail_archive_threads_mutation(
            account=account,
            thread_ids=thread_ids,
            reason=reason,
            title=title,
            context=context,
            approval_ui=approval_ui,
        )

    @mcp.tool()
    def propose_gmail_unarchive_threads(
        account: str,
        thread_ids: list[str],
        reason: str,
        title: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Propose unarchiving Gmail threads, returning a human approval URL."""
        return propose_gmail_unarchive_threads_mutation(
            account=account,
            thread_ids=thread_ids,
            reason=reason,
            title=title,
            context=context,
            approval_ui=approval_ui,
        )

    @mcp.tool()
    def propose_gmail_send_email(
        account: str,
        to: list[str],
        subject: str,
        body_text: str,
        reason: str,
        cc: list[str] | None = None,
        bcc: list[str] | None = None,
        body_html: str = "",
        reply_to_thread_id: str = "",
        delivery_mode: str = "send",
        title: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Propose a Gmail email to send or create as a draft, returning a human approval URL."""
        return propose_gmail_send_email_mutation(
            account=account,
            to=to,
            subject=subject,
            body_text=body_text,
            reason=reason,
            cc=cc,
            bcc=bcc,
            body_html=body_html,
            reply_to_thread_id=reply_to_thread_id,
            delivery_mode=delivery_mode,
            title=title,
            context=context,
            approval_ui=approval_ui,
        )

    @mcp.tool()
    def propose_contact_mutations(
        account: str,
        operations: list[dict[str, Any]],
        reason: str,
        title: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Propose a reviewed batch of contact create, update, and delete operations."""
        return propose_contact_mutations_batch(
            account=account,
            operations=operations,
            reason=reason,
            title=title,
            context=context,
            approval_ui=approval_ui,
        )

    return mcp


def propose_mutation_request_batch(
    *,
    title: str,
    reason: str,
    mutations: list[dict[str, Any]],
    approval_ui: RunningMutationApprovalUi,
    context: dict[str, Any] | None = None,
    requested_by: str = "mcp",
) -> dict[str, Any]:
    if not title.strip():
        raise ValueError("title must not be blank")
    if not reason.strip():
        raise ValueError("reason must not be blank")
    if not mutations:
        raise ValueError("mutations must include at least one mutation")

    mutation_types = [str(mutation.get("type") or mutation.get("operation") or "").strip() for mutation in mutations]
    settings = load_settings(
        require_gmail=any(
            mutation_type in {"gmail.archive_threads", "gmail.unarchive_threads", "gmail.send_email"}
            for mutation_type in mutation_types
        ),
        require_gmail_client_secrets=False,
        require_contact_mutations=any(
            mutation_type in {"google_people.contacts", "contacts.batch_mutation"} for mutation_type in mutation_types
        ),
    )
    normalized_mutations: list[dict[str, Any]] = []
    for index, mutation in enumerate(mutations):
        mutation_type = mutation_types[index]
        normalized = dict(mutation)
        if mutation_type in {"gmail.archive_threads", "gmail.unarchive_threads", "gmail.send_email"}:
            normalized["account"] = settings.account_for_email(str(mutation.get("account") or "")).email_address
        elif mutation_type in {"google_people.contacts", "contacts.batch_mutation"}:
            normalized["account"] = settings.contact_account_for_email(str(mutation.get("account") or "")).email_address
        else:
            raise ValueError(
                f"mutation {index} has unsupported type {mutation_type!r}; expected gmail.archive_threads, gmail.unarchive_threads, gmail.send_email, or google_people.contacts"
            )
        normalized_mutations.append(normalized)

    warehouse = warehouse_from_settings(settings)
    try:
        request = warehouse.propose_upstream_mutation_request(
            title=title,
            reason=reason,
            mutations=normalized_mutations,
            context=context,
            requested_by=requested_by,
        )
    finally:
        warehouse.close()

    request_id = str(request["id"])
    return {
        "request_id": request_id,
        "mutation_ids": [str(mutation["id"]) for mutation in request.get("mutations", [])],
        "approval_url": approval_ui.request_url(request_id),
        "status": str(request["status"]),
    }


def propose_mutation_request(**kwargs) -> dict[str, Any]:
    return propose_mutation_request_batch(**kwargs)


def propose_gmail_archive_threads_mutation(
    *,
    account: str,
    thread_ids: list[str],
    reason: str,
    approval_ui: RunningMutationApprovalUi,
    title: str | None = None,
    context: dict[str, Any] | None = None,
    requested_by: str = "mcp",
) -> dict[str, Any]:
    normalized_thread_ids = [str(thread_id).strip() for thread_id in thread_ids if str(thread_id).strip()]
    if not normalized_thread_ids:
        raise ValueError("thread_ids must include at least one Gmail thread ID")
    if not reason.strip():
        raise ValueError("reason must not be blank")

    settings = load_settings(require_gmail=True, require_gmail_client_secrets=False)
    gmail_account = settings.account_for_email(account)
    warehouse = warehouse_from_settings(settings)
    try:
        mutation = warehouse.propose_gmail_archive_threads(
            account=gmail_account.email_address,
            thread_ids=normalized_thread_ids,
            reason=reason,
            title=title,
            context=context,
            requested_by=requested_by,
        )
    finally:
        warehouse.close()

    request_id = str(mutation["id"])
    return {
        "request_id": request_id,
        "mutation_ids": [str(child["id"]) for child in mutation.get("mutations", [])],
        "approval_url": approval_ui.request_url(request_id),
        "status": str(mutation["status"]),
    }


def propose_gmail_unarchive_threads_mutation(
    *,
    account: str,
    thread_ids: list[str],
    reason: str,
    approval_ui: RunningMutationApprovalUi,
    title: str | None = None,
    context: dict[str, Any] | None = None,
    requested_by: str = "mcp",
) -> dict[str, Any]:
    normalized_thread_ids = [str(thread_id).strip() for thread_id in thread_ids if str(thread_id).strip()]
    if not normalized_thread_ids:
        raise ValueError("thread_ids must include at least one Gmail thread ID")
    if not reason.strip():
        raise ValueError("reason must not be blank")

    settings = load_settings(require_gmail=True, require_gmail_client_secrets=False)
    gmail_account = settings.account_for_email(account)
    warehouse = warehouse_from_settings(settings)
    try:
        mutation = warehouse.propose_gmail_unarchive_threads(
            account=gmail_account.email_address,
            thread_ids=normalized_thread_ids,
            reason=reason,
            title=title,
            context=context,
            requested_by=requested_by,
        )
    finally:
        warehouse.close()

    request_id = str(mutation["id"])
    return {
        "request_id": request_id,
        "mutation_ids": [str(child["id"]) for child in mutation.get("mutations", [])],
        "approval_url": approval_ui.request_url(request_id),
        "status": str(mutation["status"]),
    }


def propose_gmail_send_email_mutation(
    *,
    account: str,
    to: list[str],
    subject: str,
    body_text: str,
    reason: str,
    approval_ui: RunningMutationApprovalUi,
    cc: list[str] | None = None,
    bcc: list[str] | None = None,
    body_html: str = "",
    reply_to_thread_id: str = "",
    delivery_mode: str = "send",
    title: str | None = None,
    context: dict[str, Any] | None = None,
    requested_by: str = "mcp",
) -> dict[str, Any]:
    if not reason.strip():
        raise ValueError("reason must not be blank")
    message = {
        "to": [str(value).strip() for value in (to or []) if str(value).strip()],
        "cc": [str(value).strip() for value in (cc or []) if str(value).strip()],
        "bcc": [str(value).strip() for value in (bcc or []) if str(value).strip()],
        "subject": subject.strip(),
        "body_text": body_text,
        "body_html": body_html,
        "reply_to_thread_id": reply_to_thread_id.strip(),
    }
    settings = load_settings(require_gmail=True, require_gmail_client_secrets=False)
    gmail_account = settings.account_for_email(account)
    warehouse = warehouse_from_settings(settings)
    try:
        mutation = warehouse.propose_gmail_send_email(
            account=gmail_account.email_address,
            message=message,
            delivery_mode=delivery_mode,
            reason=reason,
            title=title,
            context=context,
            requested_by=requested_by,
        )
    finally:
        warehouse.close()

    request_id = str(mutation["id"])
    return {
        "request_id": request_id,
        "mutation_ids": [str(child["id"]) for child in mutation.get("mutations", [])],
        "approval_url": approval_ui.request_url(request_id),
        "status": str(mutation["status"]),
    }


def propose_contact_mutations_batch(
    *,
    account: str,
    operations: list[dict[str, Any]],
    reason: str,
    approval_ui: RunningMutationApprovalUi,
    title: str | None = None,
    context: dict[str, Any] | None = None,
    requested_by: str = "mcp",
) -> dict[str, Any]:
    if not operations:
        raise ValueError("operations must include at least one contact mutation")
    if not reason.strip():
        raise ValueError("reason must not be blank")

    settings = load_settings(require_gmail=False, require_contact_mutations=True)
    contact_account = settings.contact_account_for_email(account)
    warehouse = warehouse_from_settings(settings)
    try:
        mutation = warehouse.propose_contact_mutations(
            account=contact_account.email_address,
            operations=operations,
            reason=reason,
            title=title,
            context=context,
            requested_by=requested_by,
        )
    finally:
        warehouse.close()

    request_id = str(mutation["id"])
    return {
        "request_id": request_id,
        "mutation_ids": [str(child["id"]) for child in mutation.get("mutations", [])],
        "approval_url": approval_ui.request_url(request_id),
        "status": str(mutation["status"]),
    }


def main() -> None:
    approval_ui = start_mutation_approval_ui(mutation_approval_ui_config_from_env())
    mcp = create_mcp_server(approval_ui=approval_ui)
    try:
        mcp.run(transport=os.getenv("PDW_MCP_TRANSPORT", "stdio"))
    finally:
        approval_ui.close()


if __name__ == "__main__":
    main()
