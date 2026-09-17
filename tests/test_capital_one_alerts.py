from datetime import UTC, datetime
from decimal import Decimal

import pytest

from personal_data_warehouse.capital_one_alerts import parse_purchase_alert, reconcile_alerts

NOW = datetime(2026, 9, 16, tzinfo=UTC)


def email(**overrides):
    return {
        "account": "owner@example.test",
        "message_id": "mail-1",
        "from_address": "Capital One <capitalone@notification.capitalone.com>",
        "subject": "A new transaction was charged to your account",
        "payload_json": {
            "payload": {
                "headers": [
                    {
                        "name": "Authentication-Results",
                        "value": "mx.google.com; dkim=pass header.i=@notification.capitalone.com; dmarc=pass header.from=notification.capitalone.com",
                    }
                ]
            }
        },
        "body_text": "About your Venture X Card ending in 1234\nAs requested, we're notifying you that on Sep. 16, 2026, at SP TEST COFFEE, a pending authorization or purchase in the amount of $5.97 was placed or charged on your Venture X Card.",
        **overrides,
    }


def account(**overrides):
    return dict(
        account_id="card",
        account="owner@example.test",
        institution="Capital One",
        mask="1234",
        kind="credit",
        **overrides,
    )


def posted(id="posted", amount="-5.97", **overrides):
    return (
        dict(
            transaction_id=id,
            account_id="card",
            posted_at=NOW,
            amount=Decimal(amount),
            currency="USD",
            description="SP TEST COFFEE",
            merchant="",
            pending=0,
        )
        | overrides
    )


def test_parse_purchase():
    alert = parse_purchase_alert(email())
    assert alert["amount"] == Decimal("-5.97")
    assert alert["mask"] == "1234"
    assert alert["posted_at"] == NOW
    assert alert["merchant"] == "SP TEST COFFEE"


@pytest.mark.parametrize(
    "change",
    [
        {"from_address": "capitalone@evil.test"},
        {"subject": "You have a credit"},
        {"body_text": "unrecognized template"},
        {"body_text": email()["body_text"].replace("Sep. 16", "Sep. 99")},
    ],
)
def test_unknown_or_untrusted_template_fails_closed(change):
    assert parse_purchase_alert(email(**change)) is None


def test_unmatched_alert_is_pending_and_repeatable():
    rows, links = reconcile_alerts([email()], [account()], [], NOW, 1)
    assert len(rows) == len(links) == 1
    assert rows[0]["pending"] == 1
    assert rows[0]["source"] == "capital_one_alert"
    assert rows[0]["amount"] == Decimal("-5.97")
    assert reconcile_alerts([email()], [account()], [], NOW, 1) == (rows, links)


@pytest.mark.parametrize("source", ["plaid", "manual_finance"])
def test_posted_replaces_provisional_and_keeps_provenance(source):
    rows, links = reconcile_alerts([email()], [account()], [posted(source=source)], NOW, 1)
    assert rows == []
    assert links[0]["transaction_id"] == "posted"


def test_different_currency_or_owner_never_matches():
    assert reconcile_alerts([email(account="other@example.test")], [account()], [], NOW, 1) == ([], [])
    candidate = posted()
    candidate["currency"] = "CAD"
    rows, _ = reconcile_alerts([email()], [account()], [candidate], NOW, 1)
    assert len(rows) == 1


def test_ambiguous_or_changed_amount_is_not_guessed():
    for candidates in ([posted("a"), posted("b")], [posted(amount="-7.00")]):
        rows, links = reconcile_alerts([email()], [account()], candidates, NOW, 1)
        assert len(rows) == 1
        assert links[0]["match_method"] == "needs_review"


def test_two_alerts_cannot_claim_one_posted_transaction():
    rows, links = reconcile_alerts([email(), email(message_id="mail-2")], [account()], [posted()], NOW, 1)
    assert len(rows) == 2
    assert all(link["match_method"] == "needs_review" for link in links)


def test_duplicate_delivery_same_message_id_counts_once():
    rows, links = reconcile_alerts([email(), email()], [account()], [], NOW, 1)
    assert len(rows) == len(links) == 1


def test_missing_or_failed_gmail_authentication_is_not_financial_evidence():
    assert parse_purchase_alert(email(payload_json={})) is None
    assert (
        parse_purchase_alert(
            email(
                payload_json={
                    "payload": {
                        "headers": [
                            {
                                "name": "Authentication-Results",
                                "value": "mx.google.com; dkim=fail header.i=@notification.capitalone.com",
                            }
                        ]
                    }
                }
            )
        )
        is None
    )


def test_processor_prefix_does_not_prevent_reconciliation():
    rows, links = reconcile_alerts([email()], [account()], [posted(description="TEST COFFEE")], NOW, 1)
    assert rows == []
    assert links[0]["transaction_id"] == "posted"


def test_distinct_amounts_at_same_merchant_reconcile_independently():
    second = email(message_id="mail-2", body_text=email()["body_text"].replace("$5.97", "$6.50"))
    rows, links = reconcile_alerts([email(), second], [account()], [posted("a"), posted("b", amount="-6.50")], NOW, 1)
    assert rows == []
    assert {link["transaction_id"] for link in links} == {"a", "b"}


def test_expired_unmatched_authorization_is_unconfirmed_not_settled():
    from datetime import timedelta

    rows, links = reconcile_alerts([email()], [account()], [], NOW + timedelta(days=31), 1)
    assert rows[0]["pending"] == 1
    assert links[0]["match_method"] == "expired_unconfirmed"


@pytest.mark.parametrize(
    "changes",
    [
        {"description": "UNRELATED SHOP", "merchant": "UNRELATED SHOP"},
        {"posted_at": datetime(2026, 9, 25, tzinfo=UTC)},
        {"account_id": "other-card"},
    ],
)
def test_unrelated_bank_record_cannot_consume_alert(changes):
    rows, links = reconcile_alerts([email()], [account()], [posted(**changes)], NOW, 1)
    assert len(rows) == 1
    assert links[0]["match_method"] == "provisional"


def test_pending_plaid_record_replaces_alert_without_becoming_settled():
    rows, links = reconcile_alerts([email()], [account()], [posted(pending=1)], NOW, 1)
    assert rows == []
    assert links[0]["transaction_id"] == "posted"


def test_ambiguous_account_identity_is_withheld():
    assert reconcile_alerts([email()], [account(), account()], [], NOW, 1) == ([], [])


def test_removed_authorization_does_not_resurrect_as_live_pending_spend():
    witness = posted("pending", pending=1, removed=True, successor_transaction_id="")
    rows, links = reconcile_alerts([email()], [account()], [], NOW, 1, authorizations=[witness])
    assert len(rows) == 1  # Keep original evidence, not an expense.
    assert rows[0]["pending"] == 1
    assert links[0]["match_method"] == "authorization_removed"


def test_bank_native_successor_reconciles_changed_tip_amount():
    witness = posted("pending", pending=1, removed=True, successor_transaction_id="settled")
    settled = posted("settled", amount="-7.00")
    rows, links = reconcile_alerts([email()], [account()], [settled], NOW, 1, authorizations=[witness])
    assert rows == []
    assert links[0]["transaction_id"] == "settled"
    assert links[0]["match_method"] == "alert_pending_successor"


def test_removed_authorization_can_later_match_posted_charge():
    witness = posted("pending", pending=1, removed=True, successor_transaction_id="")
    rows, links = reconcile_alerts([email()], [account()], [posted()], NOW, 1, authorizations=[witness])
    assert rows == []
    assert links[0]["transaction_id"] == "posted"


def test_refund_is_not_used_as_the_purchase_match():
    bank = [posted(), posted("refund", amount="5.97")]
    rows, links = reconcile_alerts([email()], [account()], bank, NOW, 1)
    assert rows == []
    assert links[0]["transaction_id"] == "posted"
    assert sum(t["amount"] for t in bank if not t["pending"]) == 0
