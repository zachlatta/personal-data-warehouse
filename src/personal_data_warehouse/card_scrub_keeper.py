"""Retain a checkpoint-bound seed across database and transcript maintenance.

One process owns this keeper until verification finishes. Private callers supply
their pause/restore context and reconnect factory; neither credentials nor seed
values are serialized. Process death still requires an unsanitized seed source.
"""
from personal_data_warehouse import scrub_conversation_cards as cards
from personal_data_warehouse.card_scrub_selector import resolve_selector
from personal_data_warehouse.scrub_conversation_jsonl import scrub_jsonl


class SessionKeeper:
    def __init__(self, warehouse, plan, session):
        spec = plan['selector']
        self.retained = resolve_selector(warehouse, spec['scope'], spec['seed_rows'], expected=spec)
        self.plan = plan
        self.session = tuple(session)
        self.warehouse = warehouse
        self.selector = self.retained.seed_only(spec)
        # Validate completion before a caller can pause anything.
        cards.session_subplan(warehouse, plan, self.session, selector=self.selector)

    def reconnect(self, connect):
        """Call only after restoration. No seed/source read on the new connection."""
        replacement = connect()
        self.selector = self.retained.seed_only(self.plan['selector'])
        self.warehouse = replacement

    def apply(self, *, window, exact_source_rows, **budgets):
        return cards.apply_session(self.warehouse, self.plan, self.session,
            selector=self.selector, window=window, exact_source_rows=exact_source_rows,
            **budgets)

    def jsonl(self, root, *, apply=False):
        return scrub_jsonl(root, self.session[1], apply=apply, selector=self.selector)

    def verify(self, *, workers=1):
        """Global source/timeline/chunk check with the retained exact policy."""
        after = cards.discover(self.warehouse, selector=self.selector, workers=workers)
        if after['candidates']:
            raise RuntimeError('seed readings remain; verification incomplete')
        return after
