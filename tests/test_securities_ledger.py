"""Security-level trades and tax lots.

The cash ledger stores a brokerage buy as an anonymous debit, which is why
purchase lots older than Plaid's 730-day window were unreconstructable. These
are the pure functions that turn Plaid investment transactions and manual
statement trade rows into one deduped per-security trade fact, then reduce
those into lots.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal as D

from personal_data_warehouse.securities_ledger import (
    SecurityIdentity,
    SecurityResolver,
    build_tax_lots,
    dedupe_security_trades,
    document_trade_entry,
    normalize_cusip,
    normalize_ticker,
    option_identity,
    plaid_trade_side,
    stable_security_transaction_id,
)


def _ts(y, m, d) -> datetime:
    return datetime(y, m, d, tzinfo=UTC)


def _trade(**overrides):
    trade = {
        "account_id": "fa_1",
        "identity": SecurityIdentity(ticker="ACME", cusip="111111AA1", name="Acme Networks"),
        "trade_date": date(2020, 8, 10),
        "side": "buy",
        "quantity": D("10"),
        "price": D("40.00"),
        "amount": D("400.00"),
        "fees": D("0"),
        "currency": "USD",
        "source": "manual_finance",
        "source_row_key": "sha-a|0",
    }
    trade.update(overrides)
    return trade


# --- identity -----------------------------------------------------------------


def test_normalizers_strip_noise():
    assert normalize_ticker(" acme ") == "ACME"
    assert normalize_ticker("BRK.B") == "BRK.B"
    assert normalize_ticker("") == ""
    assert normalize_cusip(" 111111aa1 ") == "111111AA1"
    # Plaid's cash pseudo-securities are not real tickers.
    assert normalize_ticker("CUR:USD") == ""


def test_resolver_unifies_a_security_across_sources():
    """A statement prints 'Acme Networks' + CUSIP; Plaid reports ticker ACME. The
    same holding must not fork into two securities."""
    resolver = SecurityResolver()
    statement = SecurityIdentity(ticker="ACME", cusip="111111AA1", name="Acme Networks")
    plaid = SecurityIdentity(ticker="ACME", cusip="", name="Acme Networks Inc - Ordinary Shares - Class A")
    resolver.add(statement)
    resolver.add(plaid)
    assert resolver.key_for(statement) == resolver.key_for(plaid)
    # CUSIP-only evidence joins the same cluster.
    cusip_only = SecurityIdentity(ticker="", cusip="111111AA1", name="")
    resolver.add(cusip_only)
    assert resolver.key_for(cusip_only) == resolver.key_for(plaid)


def test_resolver_keeps_distinct_securities_apart():
    resolver = SecurityResolver()
    net = SecurityIdentity(ticker="ACME", cusip="111111AA1", name="Acme Networks")
    zm = SecurityIdentity(ticker="ZNTH", cusip="222222BB2", name="Zenith Systems")
    resolver.add(net)
    resolver.add(zm)
    assert resolver.key_for(net) != resolver.key_for(zm)


def test_resolver_does_not_merge_on_name_when_symbols_disagree():
    """Name is weak evidence — only used when there is no ticker or cusip."""
    resolver = SecurityResolver()
    a = SecurityIdentity(ticker="FB", cusip="", name="Facebook")
    b = SecurityIdentity(ticker="META", cusip="", name="Facebook")
    resolver.add(a)
    resolver.add(b)
    assert resolver.key_for(a) != resolver.key_for(b)
    # But two name-only sightings do merge, since nothing better exists.
    c = SecurityIdentity(ticker="", cusip="", name="Some Private Fund")
    d = SecurityIdentity(ticker="", cusip="", name="some private fund")
    resolver.add(c)
    resolver.add(d)
    assert resolver.key_for(c) == resolver.key_for(d)


def test_option_contracts_never_merge_into_the_underlying_stock():
    """A real statement row: 'ACME 09/18/2020 Call $60.00', qty 1 @ $0.30 =
    $30.00 — one contract is 100 shares, so counting it as 1 share of ACME
    would corrupt the underlying equity position."""
    resolver = SecurityResolver()
    stock = SecurityIdentity(ticker="ACME", cusip="111111AA1", name="Acme Networks")
    option = SecurityIdentity(ticker="ACME", cusip="", name="ACME 09/18/2020 Call $60.00", asset_class="option")
    resolver.add(stock)
    resolver.add(option)
    assert resolver.key_for(stock) != resolver.key_for(option)
    # Two sightings of the SAME contract still resolve together.
    same = SecurityIdentity(ticker="ACME", cusip="", name="ACME 09/18/2020 Call $60.00", asset_class="option")
    assert resolver.key_for(same) == resolver.key_for(option)


def test_distinct_option_contracts_get_distinct_identities():
    """Plaid names the SECURITY 'Nova Systems Inc' for every Nova option, putting the
    strike only in the transaction text. Keying on the security name alone
    would fuse a $50 put and a $55 put into one position."""
    resolver = SecurityResolver()
    put50 = option_identity(ticker="NOVA", text="buy 2.000 NOVA put with strike of $50.00 for $19.00 each to open")
    put55 = option_identity(ticker="NOVA", text="buy 5.000 NOVA put with strike of $55.00 for $23.00 each to open")
    call = option_identity(ticker="QBIT", text="buy 2.000 QBIT call with strike of $12.00 for $3.60 each to open")
    keys = {resolver.key_for(i) for i in (put50, put55, call)}
    assert len(keys) == 3


def test_option_underlying_is_read_from_the_text_when_plaid_omits_the_ticker():
    """Plaid gives option securities a null ticker_symbol. Falling back to the
    whole sentence would fold the PREMIUM into the identity, so a buy-to-open
    at $3.60 would never match its sell-to-close at $6.52."""
    opened = option_identity(ticker="", text="buy 2.000 QBIT call with strike of $12.00 for $3.60 each to open - PURCHASED")
    closed = option_identity(ticker="", text="sell 2.000 QBIT call with strike of $12.00 for $6.52 each to close - SOLD")
    resolver = SecurityResolver()
    assert resolver.key_for(opened) == resolver.key_for(closed)
    assert opened.ticker == "QBIT"
    # And it still resolves to the same contract when the ticker IS supplied.
    with_ticker = option_identity(ticker="QBIT", text="buy 2.000 QBIT call with strike of $12.00 for $3.60 each to open")
    assert resolver.key_for(with_ticker) == resolver.key_for(opened)


def test_the_same_option_contract_matches_across_trades_and_sources():
    """A buy-to-open and its sell-to-close are the same contract, and the
    premium differs on each — so the premium must not be part of the identity."""
    resolver = SecurityResolver()
    opened = option_identity(ticker="QBIT", text="buy 2.000 QBIT call with strike of $12.00 for $3.60 each to open")
    closed = option_identity(ticker="QBIT", text="sell 2.000 QBIT call with strike of $12.00 for $6.52 each to close")
    assert resolver.key_for(opened) == resolver.key_for(closed)


def test_option_identity_uses_expiry_when_the_document_prints_it():
    a = option_identity(ticker="ACME", text="ACME 09/18/2020 Call $60.00")
    b = option_identity(ticker="ACME", text="ACME 10/16/2020 Call $60.00")
    resolver = SecurityResolver()
    assert resolver.key_for(a) != resolver.key_for(b)


def test_document_trade_entry_classifies_option_contracts():
    entry = document_trade_entry(
        {
            "date": "2020-08-11",
            "description": "ACME 09/18/2020 Call $60.00",
            "amount": "30.00",
            "direction": "out",
            "security_name": "ACME 09/18/2020 Call $60.00",
            "ticker": "ACME",
            "cusip": "",
            "quantity": "1",
            "price_per_share": "0.30",
            "trade_side": "buy",
            "fees": "",
        }
    )
    assert entry is not None
    assert entry["identity"].asset_class == "option"
    # The price is copied as printed; the 100x contract multiplier is NOT
    # silently applied to make the arithmetic look tidy.
    assert entry["price"] == D("0.30")
    assert entry["price_is_derived"] is False


def test_document_option_derives_the_same_per_share_quote_the_statement_uses():
    entry = document_trade_entry(
        {
            "date": "2020-08-11",
            "description": "ACME 09/18/2020 Call $60.00",
            "amount": "30.00",
            "direction": "out",
            "security_name": "ACME 09/18/2020 Call $60.00",
            "ticker": "ACME",
            "cusip": "",
            "quantity": "1",
            "price_per_share": "",
            "trade_side": "buy",
            "fees": "",
        }
    )
    assert entry is not None
    assert entry["price"] == D("0.3000")
    assert entry["price_is_derived"] is True


def test_plain_equity_rows_are_not_mistaken_for_options():
    for name in ("Acme Networks", "Callaway Golf", "Putnam Municipal Opportunities", "Alphabet Class C"):
        entry = document_trade_entry(
            {
                "date": "2020-08-11",
                "description": name,
                "amount": "10.00",
                "direction": "out",
                "security_name": name,
                "ticker": "X",
                "cusip": "",
                "quantity": "1",
                "price_per_share": "10.00",
                "trade_side": "buy",
                "fees": "",
            }
        )
        assert entry is not None and entry["identity"].asset_class == "spot", name


def test_resolver_key_is_stable_across_runs():
    a, b = SecurityResolver(), SecurityResolver()
    identity = SecurityIdentity(ticker="ACME", cusip="111111AA1", name="Acme Networks")
    a.add(identity)
    b.add(identity)
    assert a.key_for(identity) == b.key_for(identity)
    assert a.key_for(identity).startswith("fsec_")


# --- source rows --------------------------------------------------------------


def test_plaid_trade_side_maps_only_real_share_movements():
    assert plaid_trade_side("buy", "buy", D("3")) == "buy"
    assert plaid_trade_side("buy", "dividend reinvestment", D("0.4")) == "buy"
    assert plaid_trade_side("sell", "sell", D("-5")) == "sell"
    # Cash events move no shares and are not trades.
    assert plaid_trade_side("cash", "dividend", D("0")) is None
    assert plaid_trade_side("fee", "margin expense", D("0")) is None
    # Share transfers open/close lots but carry no purchase price.
    assert plaid_trade_side("transfer", "transfer", D("377")) == "transfer_in"
    assert plaid_trade_side("transfer", "transfer", D("-377")) == "transfer_out"


def test_document_trade_entry_reads_statement_trade_detail():
    entry = document_trade_entry(
        {
            "date": "2020-08-07",
            "description": "Bluebox Unsolicited, CUSIP: 333333CC3",
            "amount": "1.64",
            "direction": "out",
            "security_name": "Bluebox",
            "ticker": "BLUE",
            "cusip": "333333CC3",
            "quantity": "0.079322",
            "price_per_share": "20.68",
            "trade_side": "buy",
            "fees": "",
        }
    )
    assert entry is not None
    assert entry["side"] == "buy"
    assert entry["quantity"] == D("0.079322")
    assert entry["price"] == D("20.68")
    assert entry["amount"] == D("1.64")
    assert entry["identity"].ticker == "BLUE"


def test_document_trade_entry_ignores_plain_cash_flows():
    assert (
        document_trade_entry(
            {
                "date": "2020-08-03",
                "description": "ACH Deposit",
                "amount": "1000.00",
                "direction": "in",
                "security_name": "",
                "ticker": "",
                "cusip": "",
                "quantity": "",
                "price_per_share": "",
                "trade_side": "",
                "fees": "",
            }
        )
        is None
    )


def test_document_trade_entry_infers_side_from_direction_when_unstated():
    entry = document_trade_entry(
        {
            "date": "2021-01-04",
            "description": "Apple Buy",
            "amount": "10.00",
            "direction": "out",
            "security_name": "Apple",
            "ticker": "AAPL",
            "cusip": "",
            "quantity": "0.1",
            "price_per_share": "",
            "trade_side": "",
            "fees": "",
        }
    )
    assert entry is not None and entry["side"] == "buy"
    # Price is derived only when the statement omitted it, and is marked so.
    assert entry["price"] == D("100.00")
    assert entry["price_is_derived"] is True


def test_document_trade_entry_preserves_security_transfers_without_inventing_basis():
    entry = document_trade_entry(
        {
            "date": "2021-01-04",
            "description": "ACATS transfer received",
            "amount": "",
            "direction": "in",
            "security_name": "Acme Networks",
            "ticker": "ACME",
            "cusip": "111111AA1",
            "quantity": "10",
            "price_per_share": "",
            "trade_side": "transfer_in",
            "fees": "",
        }
    )
    assert entry is not None
    assert entry["side"] == "transfer_in"
    assert entry["price"] is None
    assert entry["price_is_derived"] is False


# --- cross-source dedup -------------------------------------------------------


def test_statement_trade_merges_into_the_overlapping_plaid_trade():
    """153 statements overlap Plaid's window. Without dedup every trade in the
    overlap doubles, and doubled trades make lots silently wrong."""
    plaid = [_trade(source="plaid", source_row_key="z|investment|p1", trade_date=date(2025, 3, 3))]
    document = [_trade(source="manual_finance", source_row_key="sha-a|4", trade_date=date(2025, 3, 5))]
    rows, links, merged = dedupe_security_trades(plaid, document)
    assert merged == 1
    assert len(rows) == 1
    # Plaid wins precedence, and both source rows point at the surviving fact.
    assert rows[0]["source"] == "plaid"
    assert {link["source"] for link in links} == {"plaid", "manual_finance"}
    assert {link["transaction_id"] for link in links} == {rows[0]["transaction_id"]}
    assert [link["match_method"] for link in links if link["source"] == "manual_finance"] == [
        "security_quantity_date"
    ]


def test_trades_outside_the_date_window_stay_separate():
    plaid = [_trade(source="plaid", source_row_key="z|investment|p1", trade_date=date(2025, 3, 3))]
    document = [_trade(source="manual_finance", source_row_key="sha-a|4", trade_date=date(2025, 3, 20))]
    rows, _links, merged = dedupe_security_trades(plaid, document)
    assert merged == 0
    assert len(rows) == 2


def test_a_plaid_trade_absorbs_at_most_one_statement_row():
    """Two genuine same-size buys must not both collapse onto one Plaid row."""
    plaid = [_trade(source="plaid", source_row_key="z|investment|p1", trade_date=date(2025, 3, 3))]
    document = [
        _trade(source="manual_finance", source_row_key="sha-a|4", trade_date=date(2025, 3, 3)),
        _trade(source="manual_finance", source_row_key="sha-a|5", trade_date=date(2025, 3, 4)),
    ]
    rows, _links, merged = dedupe_security_trades(plaid, document)
    assert merged == 1
    assert len(rows) == 2


def test_different_securities_never_merge():
    plaid = [_trade(source="plaid", source_row_key="z|investment|p1")]
    document = [
        _trade(
            source="manual_finance",
            source_row_key="sha-a|4",
            identity=SecurityIdentity(ticker="ZNTH", cusip="222222BB2", name="Zenith Systems"),
        )
    ]
    rows, _links, merged = dedupe_security_trades(plaid, document)
    assert merged == 0
    assert len(rows) == 2


def test_same_quantity_and_date_with_materially_different_amounts_do_not_merge():
    """Two same-size buys can happen on one day; cash total disambiguates them."""
    plaid = [
        _trade(
            source="plaid",
            source_row_key="z|investment|p1",
            trade_date=date(2025, 3, 3),
            amount=D("400"),
        )
    ]
    document = [
        _trade(
            source="manual_finance",
            source_row_key="sha-a|4",
            trade_date=date(2025, 3, 3),
            amount=D("450"),
        )
    ]
    rows, _links, merged = dedupe_security_trades(plaid, document)
    assert merged == 0
    assert len(rows) == 2


def test_dedupe_is_deterministic_under_input_reordering():
    plaid = [
        _trade(source="plaid", source_row_key="z|investment|p1", trade_date=date(2025, 3, 3)),
        _trade(source="plaid", source_row_key="z|investment|p2", trade_date=date(2025, 4, 3)),
    ]
    document = [
        _trade(source="manual_finance", source_row_key="sha-a|4", trade_date=date(2025, 3, 4)),
        _trade(source="manual_finance", source_row_key="sha-a|5", trade_date=date(2025, 4, 4)),
    ]
    first = dedupe_security_trades(plaid, document)
    second = dedupe_security_trades(list(reversed(plaid)), list(reversed(document)))
    assert [r["transaction_id"] for r in first[0]] == [r["transaction_id"] for r in second[0]]


def test_transaction_ids_are_stable_and_source_scoped():
    a = stable_security_transaction_id("plaid", "z|investment|p1")
    assert a == stable_security_transaction_id("plaid", "z|investment|p1")
    assert a != stable_security_transaction_id("manual_finance", "z|investment|p1")
    assert a.startswith("fst_")


# --- tax lots -----------------------------------------------------------------


def test_fifo_lots_open_on_buys_and_close_oldest_first():
    key = "fsec_x"
    trades = [
        _trade(trade_date=date(2020, 8, 10), side="buy", quantity=D("10"), price=D("40"), amount=D("400")),
        _trade(trade_date=date(2021, 2, 1), side="buy", quantity=D("5"), price=D("80"), amount=D("400")),
        _trade(trade_date=date(2026, 1, 5), side="sell", quantity=D("12"), price=D("100"), amount=D("1200")),
    ]
    for trade in trades:
        trade["security_key"] = key
    lots = build_tax_lots(trades)
    assert len(lots) == 2
    first, second = sorted(lots, key=lambda lot: lot["acquired_on"])
    # Oldest lot fully consumed.
    assert first["quantity"] == D("10") and first["quantity_remaining"] == D("0")
    assert first["status"] == "closed"
    assert first["cost_basis"] == D("400")
    assert first["proceeds"] == D("1000")  # 10 shares at 100
    assert first["realized_gain"] == D("600")
    # Second lot partially consumed: 2 of 5 shares.
    assert second["quantity_remaining"] == D("3")
    assert second["status"] == "open"
    assert second["cost_basis_remaining"] == D("240")  # 3 * 80
    assert second["realized_gain"] == D("40")  # 2 * (100 - 80)


def test_lot_basis_prefers_the_trade_total_over_rounded_unit_price():
    """Fractional-share statements print the authoritative cash total to cents.

    Multiplying a six-decimal quantity by a two-decimal displayed quote can be
    a fraction of a cent away, and option statements quote a per-share premium
    while quantity counts 100-share contracts. In both cases the transaction
    amount, not displayed price * quantity, is the economic basis.
    """
    equity = {
        **_trade(quantity=D("0.079322"), price=D("20.68"), amount=D("1.64")),
        "security_key": "equity",
    }
    option = {
        **_trade(quantity=D("1"), price=D("0.30"), amount=D("30.00")),
        "security_key": "option",
    }
    lots = {lot["security_key"]: lot for lot in build_tax_lots([equity, option])}
    assert lots["equity"]["cost_basis"] == D("1.64")
    assert lots["equity"]["cost_per_unit"] == D("1.64") / D("0.079322")
    assert lots["option"]["cost_basis"] == D("30.00")
    assert lots["option"]["cost_per_unit"] == D("30.00")


def test_normalized_option_price_fallback_uses_the_contract_unit():
    option = {
        **_trade(
            quantity=D("2"),
            price=D("30.00"),
            amount=None,
            source="manual_finance",
        ),
        "security_key": "option",
        "asset_class": "option",
    }
    [lot] = build_tax_lots([option])
    assert lot["cost_per_unit"] == D("30.00")
    assert lot["cost_basis"] == D("60.00")


def test_lot_basis_and_sale_proceeds_include_transaction_fees_once():
    trades = [
        {
            **_trade(
                trade_date=date(2024, 1, 1),
                side="buy",
                quantity=D("2"),
                price=D("10"),
                amount=D("20"),
                fees=D("1.00"),
            ),
            "security_key": "k",
        },
        {
            **_trade(
                trade_date=date(2025, 1, 2),
                side="sell",
                quantity=D("2"),
                price=D("15"),
                amount=D("30"),
                fees=D("2.00"),
            ),
            "security_key": "k",
        },
    ]
    [lot] = build_tax_lots(trades)
    assert lot["cost_basis"] == D("21.00")
    assert lot["proceeds"] == D("28.00")
    assert lot["realized_gain"] == D("7.00")


def test_lot_term_follows_the_one_year_holding_rule():
    lots = build_tax_lots(
        [
            {**_trade(trade_date=date(2024, 1, 10), side="buy", quantity=D("1")), "security_key": "k"},
            {**_trade(trade_date=date(2026, 1, 10), side="buy", quantity=D("1")), "security_key": "k"},
        ],
        as_of=date(2026, 8, 14),
    )
    terms = {lot["acquired_on"]: lot["term"] for lot in lots}
    assert terms[date(2024, 1, 10)] == "long"
    assert terms[date(2026, 1, 10)] == "short"


def test_transferred_in_shares_open_a_lot_with_unknown_basis():
    """A transfer carries basis from the origin account; the transfer amount is
    NOT a purchase price. Saying otherwise would invent a cost basis."""
    lots = build_tax_lots(
        [
            {
                **_trade(trade_date=date(2022, 5, 2), side="transfer_in", quantity=D("50"), price=None, amount=None),
                "security_key": "k",
            }
        ]
    )
    assert len(lots) == 1
    assert lots[0]["basis_known"] is False
    assert lots[0]["cost_basis"] is None


def test_selling_more_than_is_held_is_reported_not_invented():
    """Partial statement coverage means sells can outrun known buys. That is a
    coverage fact to surface, never a negative lot."""
    lots = build_tax_lots(
        [
            {**_trade(trade_date=date(2021, 1, 1), side="buy", quantity=D("2"), price=D("10"), amount=D("20")), "security_key": "k"},
            {**_trade(trade_date=date(2021, 6, 1), side="sell", quantity=D("5"), price=D("30"), amount=D("150")), "security_key": "k"},
        ]
    )
    assert all(lot["quantity_remaining"] >= 0 for lot in lots)
    unmatched = [lot for lot in lots if lot["status"] == "unmatched_sale"]
    assert len(unmatched) == 1
    assert unmatched[0]["quantity"] == D("3")


def test_lots_are_per_account_and_per_security():
    trades = [
        {**_trade(account_id="fa_1", trade_date=date(2021, 1, 1), side="buy", quantity=D("1")), "security_key": "k1"},
        {**_trade(account_id="fa_2", trade_date=date(2021, 1, 1), side="buy", quantity=D("1")), "security_key": "k1"},
        {**_trade(account_id="fa_1", trade_date=date(2021, 1, 1), side="buy", quantity=D("1")), "security_key": "k2"},
    ]
    lots = build_tax_lots(trades)
    assert len(lots) == 3
    assert len({(lot["account_id"], lot["security_key"]) for lot in lots}) == 3


def test_lot_ids_are_stable_across_rebuilds():
    trades = [{**_trade(trade_date=date(2021, 1, 1), side="buy", quantity=D("1")), "security_key": "k"}]
    assert [lot["lot_id"] for lot in build_tax_lots(trades)] == [
        lot["lot_id"] for lot in build_tax_lots(trades)
    ]
