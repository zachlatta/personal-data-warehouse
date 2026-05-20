from __future__ import annotations

from personal_data_warehouse.config import Settings
from personal_data_warehouse.postgres import PostgresWarehouse


def warehouse_from_settings(settings: Settings) -> PostgresWarehouse:
    return PostgresWarehouse(settings.postgres_database_url or "")
