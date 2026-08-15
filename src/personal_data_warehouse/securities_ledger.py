"""Security-level trades and tax lots, across every finance source.

The cash ledger (``derived_finance.transactions``) records what money did: a
brokerage buy is a debit like any other. That is enough for net worth and
wrong for everything about a position — which security, how many shares, at
what price, and therefore what a lot cost and when its holding period started.

Plaid reports that detail, but only inside its 730-day maximum lookback. The
manual statement corpus reaches back to 2018 and prints the same detail per
trade. So the facts exist in two places with a ~20-month overlap, and this
module is the seam:

- ``SecurityResolver`` gives one stable id to a security seen as a ticker in
  one source and a name+CUSIP in another. Plaid carries no CUSIP, so tickers
  and CUSIPs are strong evidence and names are used only when neither exists —
  merging "Facebook" (FB) into "Facebook" (META) would silently fuse two
  distinct holdings.
- ``dedupe_security_trades`` merges a statement trade into the Plaid trade it
  describes (same account, security, side, quantity, dates within a few days),
  Plaid winning field precedence. Skipping this would double every trade in the
  overlap, and a doubled trade produces a confidently wrong lot.
- ``build_tax_lots`` reduces the unified trades into FIFO lots. It never
  invents a basis it does not have: a transferred-in position opens a lot with
  ``basis_known = False``, and a sale with no matching buy is reported as an
  ``unmatched_sale`` rather than a negative holding.

Lot *method* is a choice, not a fact — FIFO is the default and is recorded on
every row, because the broker's own election is what governs at tax time.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

# Cross-source trade dedup: a statement trade merges into a Plaid trade when
# the account, security, side and quantity match and the dates are within this
# many days (trade-vs-settlement drift across sources).
TRADE_MATCH_MAX_DAYS = 3
# Share counts are printed to different precisions by different sources
# (Plaid rounds; statements print six decimals), so quantity equality is
# relative, not exact.
QUANTITY_MATCH_TOLERANCE = Decimal("0.0001")
# Same-size buys can occur repeatedly. When both sources carry a cash total,
# require it to agree within one percent before calling them the same trade.
AMOUNT_MATCH_TOLERANCE = Decimal("0.01")

LOT_METHOD_FIFO = "fifo"

SIDE_BUY = "buy"
SIDE_SELL = "sell"
SIDE_TRANSFER_IN = "transfer_in"
SIDE_TRANSFER_OUT = "transfer_out"
SECURITY_TRADE_SIDES = (SIDE_BUY, SIDE_SELL, SIDE_TRANSFER_IN, SIDE_TRANSFER_OUT)
_OPENING_SIDES = frozenset({SIDE_BUY, SIDE_TRANSFER_IN})
_CLOSING_SIDES = frozenset({SIDE_SELL, SIDE_TRANSFER_OUT})

LOT_STATUS_OPEN = "open"
LOT_STATUS_CLOSED = "closed"
LOT_STATUS_UNMATCHED_SALE = "unmatched_sale"

# Plaid's cash pseudo-securities ("CUR:USD") and placeholder tickers are not
# holdings anyone can have a lot in.
_NON_TICKERS = frozenset({"", "CUR:USD", "N/A", "NA", "NONE", "-", "--"})
_TICKER_ALLOWED = re.compile(r"^[A-Z0-9.\-:]{1,16}$")
_CUSIP_ALLOWED = re.compile(r"^[A-Z0-9]{6,12}$")
_NAME_NOISE = re.compile(r"[^a-z0-9]+")

# Plaid investment-transaction type/subtype → the share movement it represents.
# Only rows that actually move shares are trades; dividends, interest, fees and
# cash deposits arrive through the same feed and are the cash ledger's business.
_PLAID_BUY_TYPES = frozenset({"buy"})
_PLAID_SELL_TYPES = frozenset({"sell"})
_PLAID_TRANSFER_TYPES = frozenset({"transfer"})


def normalize_ticker(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text in _NON_TICKERS or ":" in text and text.startswith("CUR"):
        return ""
    if not _TICKER_ALLOWED.match(text):
        return ""
    return text


def normalize_cusip(value: Any) -> str:
    text = str(value or "").strip().upper()
    if not _CUSIP_ALLOWED.match(text):
        return ""
    return text


def normalize_security_name(value: Any) -> str:
    return _NAME_NOISE.sub(" ", str(value or "").strip().lower()).strip()


# A directly-held unit (stock, ETF, mutual fund, or crypto), as opposed to an
# option contract. The source-specific subtype remains available in base data;
# this distinction exists to keep 100-share contracts out of spot positions.
ASSET_CLASS_SPOT = "spot"
ASSET_CLASS_OPTION = "option"

# A statement prints an option as "<TICKER> <MM/DD/YYYY> Call|Put $<strike>"
# under the underlying's ticker. One contract is 100 shares, so treating that
# row as 1 share of the underlying silently corrupts the position — the
# contract gets its own identity instead.
_OPTION_DESCRIPTION = re.compile(
    r"\b\d{1,2}/\d{1,2}/\d{2,4}\b.*\b(call|put)\b|\b(call|put)\b.*\$\s*\d",
    re.IGNORECASE,
)
# Plaid says so directly on the security.
_PLAID_OPTION_TYPES = frozenset({"derivative", "option"})


_OPTION_RIGHT = re.compile(r"\b(call|put)\b", re.IGNORECASE)
_OPTION_STRIKE = re.compile(r"strike of \$\s*([\d,]+(?:\.\d+)?)|\$\s*([\d,]+(?:\.\d+)?)\s*$", re.IGNORECASE)
_OPTION_EXPIRY = re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{2,4})\b")
# "buy 2.000 QBIT call with strike of ..." / "ACME 09/18/2020 Call $60.00"
_UNDERLYING_BEFORE_RIGHT = re.compile(r"\b([A-Za-z]{1,6})\s+(?:call|put)\b", re.IGNORECASE)
_UNDERLYING_BEFORE_EXPIRY = re.compile(r"\b([A-Za-z]{1,6})\s+\d{1,2}/\d{1,2}/\d{2,4}\b")


def option_identity(*, ticker: str, text: str) -> SecurityIdentity:
    """A canonical identity for one option CONTRACT.

    Sources describe the same contract differently: a statement prints
    "ACME 09/18/2020 Call $60.00", while Plaid puts only the strike in a
    sentence ("buy 2.000 QBIT call with strike of $12.00 for $3.60 each to
    open") and names the security after the underlying company. Two things
    must both hold — different strikes/expiries must NOT share an identity,
    and the same contract's open and close must, even though the premium and
    contract count differ between them. So the key is underlying + right +
    strike + expiry-if-printed, and nothing else.
    """
    blob = str(text or "")
    right = _OPTION_RIGHT.search(blob)
    strike_match = _OPTION_STRIKE.search(blob)
    strike = ""
    if strike_match:
        strike = (strike_match.group(1) or strike_match.group(2) or "").replace(",", "")
        if strike:
            try:
                strike = f"{Decimal(strike):.2f}"
            except InvalidOperation:
                strike = ""
    expiry = _OPTION_EXPIRY.search(blob)
    # Plaid gives option securities a null ticker_symbol, so the underlying has
    # to be read out of the text. Falling back to the whole sentence would fold
    # the premium ("for $3.60 each") into the identity, and a buy-to-open would
    # never match its own sell-to-close.
    underlying = normalize_ticker(ticker) or _option_underlying(blob)
    parts = [
        underlying or normalize_security_name(blob),
        (right.group(1).lower() if right else ""),
        strike,
        ("/".join(expiry.groups()) if expiry else ""),
    ]
    descriptor = " ".join(part for part in parts if part)
    return SecurityIdentity(
        ticker=underlying,
        cusip="",
        name=descriptor or blob,
        asset_class=ASSET_CLASS_OPTION,
    )


def _option_underlying(text: str) -> str:
    """The underlying symbol printed just before the right or the expiry."""
    for pattern in (_UNDERLYING_BEFORE_RIGHT, _UNDERLYING_BEFORE_EXPIRY):
        match = pattern.search(text or "")
        if match:
            candidate = normalize_ticker(match.group(1))
            if candidate:
                return candidate
    return ""


def classify_asset_class(*, name: str = "", description: str = "", plaid_type: str = "") -> str:
    if str(plaid_type or "").strip().lower() in _PLAID_OPTION_TYPES:
        return ASSET_CLASS_OPTION
    for text in (name, description):
        if text and _OPTION_DESCRIPTION.search(str(text)):
            return ASSET_CLASS_OPTION
    return ASSET_CLASS_SPOT


@dataclass(frozen=True)
class SecurityIdentity:
    """What one source row says about which security it means."""

    ticker: str = ""
    cusip: str = ""
    name: str = ""
    asset_class: str = ASSET_CLASS_SPOT

    def normalized(self) -> "SecurityIdentity":
        return SecurityIdentity(
            ticker=normalize_ticker(self.ticker),
            cusip=normalize_cusip(self.cusip),
            name=normalize_security_name(self.name),
            asset_class=self.asset_class,
        )

    @property
    def is_empty(self) -> bool:
        normalized = self.normalized()
        return not (normalized.ticker or normalized.cusip or normalized.name)


def _security_key(token: str) -> str:
    return f"fsec_{hashlib.sha256(token.encode()).hexdigest()[:20]}"


class SecurityResolver:
    """Union-find over security evidence, so one holding gets one id.

    Ticker and CUSIP are strong evidence and merge freely. A name merges only
    with other name-only sightings: two sources naming the same company under
    different tickers are a rename or a different share class, not one holding.
    """

    def __init__(self) -> None:
        self._parent: dict[str, str] = {}

    # --- union-find ---------------------------------------------------------
    def _find(self, token: str) -> str:
        root = self._parent.setdefault(token, token)
        while root != self._parent[root]:
            self._parent[root] = self._parent[self._parent[root]]
            root = self._parent[root]
        return root

    def _union(self, left: str, right: str) -> None:
        left_root, right_root = self._find(left), self._find(right)
        if left_root == right_root:
            return
        # Deterministic: the lexicographically smaller root always wins, so the
        # cluster is independent of the order rows arrived in.
        winner, loser = sorted((left_root, right_root))
        self._parent[loser] = winner

    def _tokens(self, identity: SecurityIdentity) -> list[str]:
        normalized = identity.normalized()
        if normalized.asset_class == ASSET_CLASS_OPTION and normalized.name:
            # An option is identified by its full contract descriptor alone.
            # Its ticker is the UNDERLYING's, so letting the ticker merge would
            # fold contracts into the stock position.
            return [f"option:{normalized.name}"]
        tokens: list[str] = []
        if normalized.cusip:
            tokens.append(f"cusip:{normalized.cusip}")
        if normalized.ticker:
            tokens.append(f"ticker:{normalized.ticker}")
        if not tokens and normalized.name:
            tokens.append(f"name:{normalized.name}")
        return tokens

    # --- public -------------------------------------------------------------
    def add(self, identity: SecurityIdentity) -> None:
        tokens = self._tokens(identity)
        if not tokens:
            return
        first = tokens[0]
        self._find(first)
        for token in tokens[1:]:
            self._union(first, token)

    def key_for(self, identity: SecurityIdentity) -> str:
        tokens = self._tokens(identity)
        if not tokens:
            return ""
        self.add(identity)
        roots = {self._find(token) for token in tokens}
        # The key is derived from the cluster's smallest root token, so it is
        # stable across runs and independent of how the cluster was built.
        return _security_key(min(roots))

    def canonical(self, identity: SecurityIdentity) -> SecurityIdentity:
        return identity.normalized()


def stable_security_transaction_id(source: str, source_row_key: str) -> str:
    """Deterministic id for one security trade, from its founding source row."""
    digest = hashlib.sha256(f"{source}|{source_row_key}".encode()).hexdigest()
    return f"fst_{digest[:24]}"


def stable_tax_lot_id(account_id: str, security_key: str, acquired_on: Any, source_row_key: str) -> str:
    token = f"{account_id}|{security_key}|{acquired_on}|{source_row_key}"
    return f"flot_{hashlib.sha256(token.encode()).hexdigest()[:24]}"


def plaid_trade_side(type_: Any, subtype: Any, quantity: Any) -> str | None:
    """Which share movement a Plaid investment transaction represents, if any."""
    kind = str(type_ or "").strip().lower()
    quantity_value = _decimal(quantity) or Decimal("0")
    if kind in _PLAID_BUY_TYPES:
        return SIDE_BUY
    if kind in _PLAID_SELL_TYPES:
        return SIDE_SELL
    if kind in _PLAID_TRANSFER_TYPES:
        if quantity_value == 0:
            return None
        return SIDE_TRANSFER_IN if quantity_value > 0 else SIDE_TRANSFER_OUT
    return None


def document_trade_entry(entry: Mapping[str, Any]) -> dict[str, Any] | None:
    """One statement transaction row → a trade, or None when it is plain cash.

    A row is a trade only when it names a security AND moves a share count;
    everything else (deposits, interest, card spend) belongs to the cash ledger
    alone. Absence is expressed by empty strings, per the extraction contract.
    """
    name = str(entry.get("security_name", ""))
    description = str(entry.get("description", ""))
    if classify_asset_class(name=name, description=description) == ASSET_CLASS_OPTION:
        identity = option_identity(
            ticker=str(entry.get("ticker", "")), text=name or description
        )
    else:
        identity = SecurityIdentity(
            ticker=str(entry.get("ticker", "")),
            cusip=str(entry.get("cusip", "")),
            name=name,
        )
    if identity.is_empty:
        return None
    quantity = _decimal(entry.get("quantity"))
    if quantity is None or quantity == 0:
        return None
    quantity = abs(quantity)

    side = str(entry.get("trade_side", "")).strip().lower()
    if side not in SECURITY_TRADE_SIDES:
        # The statement named a security and a share count but not a side.
        # Money leaving the account bought shares; money arriving sold them.
        direction = str(entry.get("direction", "")).strip().lower()
        if direction == "out":
            side = SIDE_BUY
        elif direction == "in":
            side = SIDE_SELL
        else:
            return None

    amount = _decimal(entry.get("amount"))
    price = _decimal(entry.get("price_per_share"))
    price_is_derived = False
    if (price is None or price == 0) and side in {SIDE_BUY, SIDE_SELL}:
        # Only ever derived when the document did not print a price, and
        # flagged so a reader can tell a copied price from a computed one.
        if amount is not None and quantity != 0:
            denominator = quantity * (
                Decimal("100")
                if identity.asset_class == ASSET_CLASS_OPTION
                else Decimal("1")
            )
            price = (abs(amount) / denominator).quantize(Decimal("0.0001"))
            price_is_derived = True
        else:
            price = None
    return {
        "identity": identity,
        "side": side,
        "quantity": quantity,
        "price": price,
        "price_is_derived": price_is_derived,
        "amount": abs(amount) if amount is not None else None,
        "fees": _decimal(entry.get("fees")) or Decimal("0"),
    }


def dedupe_security_trades(
    plaid_trades: Sequence[Mapping[str, Any]],
    document_trades: Sequence[Mapping[str, Any]],
    *,
    resolver: SecurityResolver | None = None,
    max_day_gap: int = TRADE_MATCH_MAX_DAYS,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    """Merge the statement/Plaid overlap into one trade fact per real trade.

    Returns ``(trade_rows, link_rows, merged_count)``. Plaid wins field
    precedence; every source row gets a link row pointing at the surviving
    trade, so the provenance of a merged fact stays inspectable.
    """
    resolver = resolver or SecurityResolver()
    for trade in list(plaid_trades) + list(document_trades):
        resolver.add(trade["identity"])

    trade_rows: dict[str, dict[str, Any]] = {}
    link_rows: list[dict[str, Any]] = []
    merged = 0

    # Sorting makes the outcome independent of input order: the same statement
    # row must always land on the same Plaid row.
    pool: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for trade in sorted(plaid_trades, key=lambda t: (str(t["source_row_key"]),)):
        security_key = resolver.key_for(trade["identity"])
        row = _trade_row(trade, security_key=security_key, resolver=resolver)
        trade_rows[row["transaction_id"]] = row
        link_rows.append(
            _link_row(trade, transaction_id=row["transaction_id"], match_method="source_id", match_score=1.0)
        )
        pool.setdefault((str(trade["account_id"]), security_key, str(trade["side"])), []).append(
            {
                "transaction_id": row["transaction_id"],
                "trade_date": trade["trade_date"],
                "quantity": _decimal(trade.get("quantity")) or Decimal("0"),
                "amount": _decimal(trade.get("amount")),
                "used": False,
            }
        )

    for trade in sorted(document_trades, key=lambda t: (str(t["source_row_key"]),)):
        security_key = resolver.key_for(trade["identity"])
        match = _best_trade_match(
            pool,
            account_id=str(trade["account_id"]),
            security_key=security_key,
            side=str(trade["side"]),
            quantity=_decimal(trade.get("quantity")) or Decimal("0"),
            amount=_decimal(trade.get("amount")),
            trade_date=trade["trade_date"],
            max_day_gap=max_day_gap,
        )
        if match is not None:
            match["used"] = True
            merged += 1
            link_rows.append(
                _link_row(
                    trade,
                    transaction_id=match["transaction_id"],
                    match_method="security_quantity_date",
                    match_score=_match_score(match["trade_date"], trade["trade_date"], max_day_gap),
                )
            )
            continue
        row = _trade_row(trade, security_key=security_key, resolver=resolver)
        trade_rows[row["transaction_id"]] = row
        link_rows.append(
            _link_row(trade, transaction_id=row["transaction_id"], match_method="source_id", match_score=1.0)
        )

    ordered = sorted(trade_rows.values(), key=lambda row: (row["trade_date"], row["transaction_id"]))
    return ordered, link_rows, merged


def build_tax_lots(
    trades: Iterable[Mapping[str, Any]],
    *,
    method: str = LOT_METHOD_FIFO,
    as_of: date | None = None,
) -> list[dict[str, Any]]:
    """Reduce unified trades into holding lots, oldest-consumed-first.

    Never invents a basis: a transferred-in lot records ``basis_known=False``
    (its real basis lives at the origin account), and a sale with no open lot
    to draw from becomes an ``unmatched_sale`` row — the honest signal that
    statement coverage is incomplete, rather than a negative position.
    """
    if method != LOT_METHOD_FIFO:
        raise ValueError(f"unsupported lot method: {method}")

    grouped: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
    for trade in trades:
        key = (str(trade["account_id"]), str(trade["security_key"]))
        grouped.setdefault(key, []).append(trade)

    lots: list[dict[str, Any]] = []
    for (account_id, security_key), group in sorted(grouped.items()):
        open_lots: list[dict[str, Any]] = []
        for trade in sorted(group, key=lambda t: (t["trade_date"], str(t.get("source_row_key", "")))):
            side = str(trade["side"])
            quantity = abs(_decimal(trade.get("quantity")) or Decimal("0"))
            if quantity == 0:
                continue
            if side in _OPENING_SIDES:
                basis = _opening_basis(trade, quantity=quantity)
                basis_known = side == SIDE_BUY and basis is not None
                cost_per_unit = basis / quantity if basis_known else None
                lot = {
                    "lot_id": stable_tax_lot_id(
                        account_id, security_key, trade["trade_date"], str(trade.get("source_row_key", ""))
                    ),
                    "account_id": account_id,
                    "security_key": security_key,
                    "acquired_on": trade["trade_date"],
                    "acquired_source": str(trade.get("source", "")),
                    "opening_transaction_id": str(trade.get("transaction_id", "")),
                    "method": method,
                    "quantity": quantity,
                    "quantity_remaining": quantity,
                    "cost_per_unit": cost_per_unit,
                    "cost_basis": basis,
                    "cost_basis_remaining": basis,
                    "basis_known": basis_known,
                    "proceeds": Decimal("0"),
                    "realized_gain": Decimal("0") if basis_known else None,
                    "disposed_on": None,
                    "status": LOT_STATUS_OPEN,
                }
                lots.append(lot)
                open_lots.append(lot)
                continue
            if side not in _CLOSING_SIDES:
                continue

            remaining = quantity
            proceeds_per_unit = _closing_proceeds_per_unit(trade, quantity=quantity)
            while remaining > 0 and open_lots:
                lot = open_lots[0]
                take = min(remaining, lot["quantity_remaining"])
                if take <= 0:
                    open_lots.pop(0)
                    continue
                lot["quantity_remaining"] -= take
                remaining -= take
                if proceeds_per_unit is not None and side == SIDE_SELL:
                    proceeds = proceeds_per_unit * take
                    lot["proceeds"] += proceeds
                    if lot["basis_known"]:
                        lot["realized_gain"] += proceeds - (lot["cost_per_unit"] * take)
                if lot["basis_known"]:
                    lot["cost_basis_remaining"] = lot["cost_per_unit"] * lot["quantity_remaining"]
                if lot["quantity_remaining"] == 0:
                    lot["status"] = LOT_STATUS_CLOSED
                    lot["disposed_on"] = trade["trade_date"]
                    open_lots.pop(0)
            if remaining > 0:
                # Sold shares we have no acquisition record for. Surfacing the
                # gap beats fabricating a lot to balance the books.
                lots.append(
                    {
                        "lot_id": stable_tax_lot_id(
                            account_id,
                            security_key,
                            trade["trade_date"],
                            f"unmatched|{trade.get('source_row_key', '')}",
                        ),
                        "account_id": account_id,
                        "security_key": security_key,
                        "acquired_on": None,
                        "acquired_source": "",
                        "opening_transaction_id": "",
                        "method": method,
                        "quantity": remaining,
                        "quantity_remaining": Decimal("0"),
                        "cost_per_unit": None,
                        "cost_basis": None,
                        "cost_basis_remaining": None,
                        "basis_known": False,
                        "proceeds": (
                            proceeds_per_unit * remaining
                            if proceeds_per_unit is not None
                            else Decimal("0")
                        ),
                        "realized_gain": None,
                        "disposed_on": trade["trade_date"],
                        "status": LOT_STATUS_UNMATCHED_SALE,
                    }
                )

    for lot in lots:
        lot["term"] = _holding_term(lot, as_of=as_of)
    return lots


# --- internals ----------------------------------------------------------------


def _trade_row(
    trade: Mapping[str, Any], *, security_key: str, resolver: SecurityResolver
) -> dict[str, Any]:
    identity = resolver.canonical(trade["identity"])
    price = _decimal(trade.get("price"))
    # Statement option tables quote premium per underlying share, while the
    # ledger quantity is contracts. Normalize to cost per contract here so the
    # stored `price` has one meaning for every asset class: price per quantity
    # unit. Plaid option prices already use the contract unit.
    if (
        price is not None
        and str(trade.get("source", "")) == "manual_finance"
        and identity.asset_class == ASSET_CLASS_OPTION
    ):
        price *= Decimal("100")
    return {
        "transaction_id": stable_security_transaction_id(
            str(trade["source"]), str(trade["source_row_key"])
        ),
        "account_id": str(trade["account_id"]),
        "security_key": security_key,
        "ticker": identity.ticker,
        "cusip": identity.cusip,
        "security_name": str(trade["identity"].name or "").strip(),
        "asset_class": identity.asset_class,
        "trade_date": trade["trade_date"],
        "side": str(trade["side"]),
        "quantity": abs(_decimal(trade.get("quantity")) or Decimal("0")),
        "price": price,
        "amount": _decimal(trade.get("amount")),
        "fees": _decimal(trade.get("fees")) or Decimal("0"),
        "currency": str(trade.get("currency", "") or ""),
        "price_is_derived": 1 if trade.get("price_is_derived") else 0,
        "source": str(trade["source"]),
    }


def _link_row(
    trade: Mapping[str, Any], *, transaction_id: str, match_method: str, match_score: float
) -> dict[str, Any]:
    return {
        "source": str(trade["source"]),
        "source_row_key": str(trade["source_row_key"]),
        "transaction_id": transaction_id,
        "match_method": match_method,
        "match_score": match_score,
    }


def _best_trade_match(
    pool: dict[tuple[str, str, str], list[dict[str, Any]]],
    *,
    account_id: str,
    security_key: str,
    side: str,
    quantity: Decimal,
    amount: Decimal | None,
    trade_date: date,
    max_day_gap: int,
) -> dict[str, Any] | None:
    candidates = pool.get((account_id, security_key, side))
    if not candidates:
        return None
    best: tuple[int, Decimal, str] | None = None
    chosen: dict[str, Any] | None = None
    for candidate in candidates:
        if candidate["used"]:
            continue
        if not _quantities_match(candidate["quantity"], quantity):
            continue
        if not _amounts_match(candidate["amount"], amount):
            continue
        gap = abs((candidate["trade_date"] - trade_date).days)
        if gap > max_day_gap:
            continue
        rank = (gap, _relative_gap(candidate["amount"], amount), candidate["transaction_id"])
        if best is None or rank < best:
            best, chosen = rank, candidate
    return chosen


def _quantities_match(left: Decimal, right: Decimal) -> bool:
    left, right = abs(left), abs(right)
    if left == right:
        return True
    larger = max(left, right)
    if larger == 0:
        return False
    return abs(left - right) / larger <= QUANTITY_MATCH_TOLERANCE


def _amounts_match(left: Decimal | None, right: Decimal | None) -> bool:
    if left is None or right is None or left == 0 or right == 0:
        return True
    return _relative_gap(left, right) <= AMOUNT_MATCH_TOLERANCE


def _relative_gap(left: Decimal | None, right: Decimal | None) -> Decimal:
    if left is None or right is None:
        return Decimal("0")
    left, right = abs(left), abs(right)
    larger = max(left, right)
    return abs(left - right) / larger if larger else Decimal("0")


def _match_score(left: date, right: date, max_day_gap: int) -> float:
    gap = abs((left - right).days)
    return round(1.0 - gap / (max_day_gap + 1), 4)


def _holding_term(lot: Mapping[str, Any], *, as_of: date | None) -> str:
    acquired_on = lot.get("acquired_on")
    if acquired_on is None:
        return ""
    end = lot.get("disposed_on") or as_of
    if end is None:
        return ""
    # The IRS long-term rule is more than one year, counted from the day after
    # acquisition — i.e. a sale on the anniversary is still short-term.
    return "long" if (end - acquired_on).days > 365 else "short"


def _opening_basis(trade: Mapping[str, Any], *, quantity: Decimal) -> Decimal | None:
    """Economic cost of an opening buy, including acquisition fees.

    ``amount`` is authoritative when present. Fractional-share statements
    round the displayed quote, so ``price * quantity`` can drift from the cash
    total, and option statements quote a per-share premium while quantity is a
    count of 100-share contracts. Both are handled correctly by preferring the
    source's total. Price is only a fallback for sources that omit amount.
    """
    amount = _decimal(trade.get("amount"))
    price = _decimal(trade.get("price"))
    fees = abs(_decimal(trade.get("fees")) or Decimal("0"))
    if amount is not None and amount != 0:
        return abs(amount) + fees
    if price is not None and price != 0:
        return abs(price) * quantity + fees
    return None


def _closing_proceeds_per_unit(
    trade: Mapping[str, Any], *, quantity: Decimal
) -> Decimal | None:
    """Net sale proceeds per unit, after disposition fees."""
    amount = _decimal(trade.get("amount"))
    price = _decimal(trade.get("price"))
    fees = abs(_decimal(trade.get("fees")) or Decimal("0"))
    if amount is not None and amount != 0:
        return (abs(amount) - fees) / quantity
    if price is not None and price != 0:
        return abs(price) - fees / quantity
    return None


def _decimal(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, float):
        return Decimal(str(value))
    text = str(value).strip().replace(",", "").replace("$", "")
    if not text:
        return None
    negative = text.startswith("(") and text.endswith(")")
    if negative:
        text = text[1:-1]
    try:
        parsed = Decimal(text)
    except InvalidOperation:
        return None
    return -parsed if negative else parsed
