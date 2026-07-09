from __future__ import annotations

import pytest

from personal_data_warehouse.readonly_sql import validate_readonly_sql


def test_validate_readonly_sql_rejects_select_into() -> None:
    with pytest.raises(ValueError):
        validate_readonly_sql("SELECT * INTO evil FROM gmail_messages")


def test_validate_readonly_sql_allows_plain_select() -> None:
    validate_readonly_sql("SELECT 1")
