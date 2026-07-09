"""Unit tests for the Snowflake connector."""

from __future__ import annotations

import pytest
import snowflake.sqlalchemy.custom_types as sct
from sqlalchemy import types
from sqlalchemy.exc import NoSuchTableError

from target_snowflake.connector import SnowflakeConnector, SnowflakeTimestampType
from target_snowflake.snowflake_types import NUMBER, VARIANT


@pytest.fixture
def connector():
    return SnowflakeConnector()


@pytest.mark.parametrize(
    ("schema", "expected_type"),
    [
        pytest.param({"type": "object"}, VARIANT, id="object"),
        pytest.param({"type": ["array", "null"]}, VARIANT, id="array"),
        pytest.param({"type": ["array", "object", "string"]}, VARIANT, id="array_object_string"),
        pytest.param({"type": ["integer", "null"]}, NUMBER, id="integer"),
        pytest.param({"type": ["number", "null"]}, sct.DOUBLE, id="number"),
        pytest.param({"type": ["string", "null"], "format": "date-time"}, sct.TIMESTAMP_NTZ, id="date-time"),
        # Upstream types
        pytest.param({"type": ["string", "null"]}, types.VARCHAR, id="string"),
        pytest.param({"type": ["boolean", "null"]}, types.BOOLEAN, id="boolean"),
        pytest.param({"type": "string", "format": "time"}, types.TIME, id="time"),
        pytest.param({"type": "string", "format": "date"}, types.DATE, id="date"),
        pytest.param({"type": "string", "format": "uuid"}, types.UUID, id="uuid"),
    ],
)
def test_jsonschema_to_sql(connector: SnowflakeConnector, schema: dict, expected_type: type[types.TypeEngine]):
    sql_type = connector.to_sql_type(schema)
    assert isinstance(sql_type, expected_type)


@pytest.mark.parametrize(
    ("config", "expected_type"),
    [
        ({"timestamp_type": SnowflakeTimestampType.TIMESTAMP_TZ}, sct.TIMESTAMP_TZ),
        ({"timestamp_type": SnowflakeTimestampType.TIMESTAMP_LTZ}, sct.TIMESTAMP_LTZ),
        ({"timestamp_type": SnowflakeTimestampType.TIMESTAMP_NTZ}, sct.TIMESTAMP_NTZ),
    ],
)
def test_datetime_to_sql(connector: SnowflakeConnector, config: dict, expected_type: type[types.TypeEngine]):
    connector.config.update(config)
    schema = {"type": ["string", "null"], "format": "date-time"}
    sql_type = connector.to_sql_type(schema)
    assert isinstance(sql_type, expected_type)


def test_to_sql_type_with_max_varchar_length(connector: SnowflakeConnector):
    sql_type = connector.to_sql_type({"type": "string", "maxLength": 1_000_000})
    assert isinstance(sql_type, types.VARCHAR)
    assert sql_type.length == 1_000_000

    sql_type = connector.to_sql_type({"type": "string", "maxLength": SnowflakeConnector.max_varchar_length + 1})
    assert isinstance(sql_type, types.VARCHAR)
    assert sql_type.length == SnowflakeConnector.max_varchar_length


def test_email_format(connector: SnowflakeConnector):
    sql_type = connector.to_sql_type({"type": "string", "format": "email"})
    assert isinstance(sql_type, types.VARCHAR)
    assert sql_type.length == 254


def test_uri_format(connector: SnowflakeConnector):
    sql_type = connector.to_sql_type({"type": "string", "format": "uri"})
    assert isinstance(sql_type, types.VARCHAR)
    assert sql_type.length == 2083


def test_hostname_format(connector: SnowflakeConnector):
    sql_type = connector.to_sql_type({"type": "string", "format": "hostname"})
    assert isinstance(sql_type, types.VARCHAR)
    assert sql_type.length == 253


def test_ipv4_format(connector: SnowflakeConnector):
    sql_type = connector.to_sql_type({"type": "string", "format": "ipv4"})
    assert isinstance(sql_type, types.VARCHAR)
    assert sql_type.length == 15


def test_ipv6_format(connector: SnowflakeConnector):
    sql_type = connector.to_sql_type({"type": "string", "format": "ipv6"})
    assert isinstance(sql_type, types.VARCHAR)
    assert sql_type.length == 45


def test_singer_decimal(connector: SnowflakeConnector):
    sql_type = connector.to_sql_type(
        {
            "type": "string",
            "format": "x-singer.decimal",
            "precision": 38,
            "scale": 18,
        },
    )
    assert isinstance(sql_type, types.DECIMAL)
    assert sql_type.precision == 38
    assert sql_type.scale == 18


_COLUMN_ROW = [{"name": "Id", "type": types.VARCHAR(16_777_216), "nullable": True}]


class _StaleSchemaInspector:
    """Models the reused inspector's stale schema reflection. snowflake-
    sqlalchemy caches _get_schema_columns() per schema in ``info_cache``; the
    first table reflected in a load freezes that map, so a table CREATEd later
    in the same load is missing and get_columns() raises a bare
    NoSuchTableError -- until ``info_cache`` is cleared and the schema is
    re-reflected. Here a non-empty ``info_cache`` stands for that stale map."""

    def __init__(self):
        self.info_cache: dict = {"schema_columns": {"ACCOUNT"}}  # stale: created before BILL
        self.calls: list[str] = []

    def get_columns(self, table_name, schema_name=None, **_kwargs):
        self.calls.append(table_name)
        if self.info_cache:
            # stale cached map does not contain the just-created table
            raise NoSuchTableError(table_name)
        return _COLUMN_ROW  # after info_cache.clear() -> fresh reflection resolves it


class _FreshInspector:
    def __init__(self):
        self.info_cache: dict = {}
        self.calls: list[str] = []

    def get_columns(self, table_name, schema_name=None, **_kwargs):
        self.calls.append(table_name)
        return _COLUMN_ROW


def test_get_table_columns_clears_stale_inspector_cache(connector: SnowflakeConnector):
    """A table CREATEd after the reused inspector cached the schema map misses;
    the fix clears info_cache and re-reflects, which resolves it."""
    fake = _StaleSchemaInspector()
    connector._inspector = fake

    columns = connector.get_table_columns('"MYDB"."MYSCHEMA"."BILL"')

    assert "Id" in columns
    assert len(fake.calls) == 2, "expected a stale miss then a post-clear retry"
    assert fake.info_cache == {}, "the stale reflection cache must be cleared"


def test_get_table_columns_no_retry_when_reflection_succeeds(connector: SnowflakeConnector):
    """When the first reflection resolves, there must be no cache clear/retry."""
    fake = _FreshInspector()
    connector._inspector = fake

    columns = connector.get_table_columns('"MYDB"."MYSCHEMA"."BILL"')

    assert "Id" in columns
    assert len(fake.calls) == 1
