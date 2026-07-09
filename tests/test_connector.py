"""Unit tests for the Snowflake connector."""

from __future__ import annotations

import pytest
import snowflake.sqlalchemy.custom_types as sct
from snowflake.sqlalchemy.snowdialect import SnowflakeDialect
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


class _FoldedSchemaInspector:
    """Models snowflake-sqlalchemy reflection of a table created under
    QUOTED_IDENTIFIERS_IGNORE_CASE=TRUE. Snowflake folds the identifier to
    upper-case at CREATE, so information_schema returns the stored name (e.g.
    ``BILL``). snowdialect keys its column dict by ``normalize_name(<stored>)``
    and raises a bare ``NoSuchTableError`` when ``normalize_name(<lookup>)`` is
    absent -- the exact behaviour that crashed the loader. Uses the real
    dialect ``normalize_name`` so the test is not circular."""

    def __init__(self, stored_name: str = "BILL"):
        normalize = SnowflakeDialect().normalize_name
        self._normalize = normalize
        self._key = normalize(stored_name)
        self.calls: list[str] = []

    def get_columns(self, table_name, schema_name=None, **_kwargs):
        self.calls.append(table_name)
        if self._normalize(table_name) != self._key:
            raise NoSuchTableError(table_name)
        return _COLUMN_ROW


class _AlwaysOkInspector:
    def __init__(self):
        self.calls: list[str] = []

    def get_columns(self, table_name, schema_name=None, **_kwargs):
        self.calls.append(table_name)
        return _COLUMN_ROW


def test_get_table_columns_resolves_case_folded_table(connector: SnowflakeConnector):
    """A mixed-case stream stored upper-case (QUOTED_IDENTIFIERS_IGNORE_CASE) is
    reflected by its quoted name, misses, and is resolved by the de-quoted +
    upper-cased retry -- verified against the real dialect normalize_name."""
    fake = _FoldedSchemaInspector(stored_name="BILL")
    connector._inspector = fake

    columns = connector.get_table_columns('"MYDB"."MYSCHEMA"."Bill"')

    assert "Id" in columns
    assert len(fake.calls) == 2, "expected a quoted miss then a de-quoted retry"
    assert '"' in fake.calls[0], "first attempt uses the quoted identifier"
    assert fake.calls[1] == fake.calls[0].strip('"').upper()


def test_get_table_columns_no_retry_when_reflection_succeeds(connector: SnowflakeConnector):
    """When the first reflection resolves, there must be no second lookup."""
    fake = _AlwaysOkInspector()
    connector._inspector = fake

    columns = connector.get_table_columns('"MYDB"."MYSCHEMA"."Bill"')

    assert "Id" in columns
    assert len(fake.calls) == 1
