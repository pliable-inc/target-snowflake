"""Snowflake target class."""

from __future__ import annotations

import logging.config

import click
from singer_sdk import typing as th
from singer_sdk.target_base import SQLTarget

from target_snowflake.connector import DEFAULT_TIMESTAMP_TYPE, SnowflakeTimestampType
from target_snowflake.initializer import initializer
from target_snowflake.sinks import SnowflakeSink

logging.config.dictConfig(
    {
        "version": 1,
        "disable_existing_loggers": False,
        "loggers": {"snowflake.connector": {"level": "WARNING"}},
    },
)

def overriden_get_sink(
        self,
        stream_name: str,
        *,
        record: dict | None = None,
        schema: dict | None = None,
        key_properties: t.Sequence[str] | None = None,
    ) -> Sink:
        """Return a sink for the given stream name.

        A new sink will be created if `schema` is provided and if either `schema` or
        `key_properties` has changed. If so, the old sink becomes archived and held
        until the next drain_all() operation.

        Developers only need to override this method if they want to provide a different
        sink depending on the values within the `record` object. Otherwise, please see
        `default_sink_class` property and/or the `get_sink_class()` method.

        Raises :class:`singer_sdk.exceptions.RecordsWithoutSchemaException` if sink does
        not exist and schema is not sent.

        Args:
            stream_name: Name of the stream.
            record: Record being processed.
            schema: Stream schema.
            key_properties: Primary key of the stream.

        Returns:
            The sink used for this target.
        """
        _ = record  # Custom implementations may use record in sink selection.
        if schema is None:
            self._assert_sink_exists(stream_name)
            return self._sinks_active[stream_name]

        existing_sink = self._sinks_active.get(stream_name, None)
        if not existing_sink:
            return self.add_sqlsink(stream_name, schema, key_properties)

        greedy_sink = os.enviorn.get('GREEDY_SINK', 'false') == 'true'

        if not greedy_sink:
            if (
                existing_sink.schema != schema
                or existing_sink.key_properties != key_properties
            ):
                if existing_sink.schema != schema: 
                    self.logger.info(f"schema diff: {existing_sink.schema} _ {schema}")
                if existing_sink.key_properties != key_properties: 
                    self.logger.info(f"prop diff: {existing_sink.key_properties} _ {key_properties}")
                
                self.logger.info(
                    "Schema or key properties for '%s' stream have changed. "
                    "Initializing a new '%s' sink...",
                    stream_name,
                    stream_name,
                )
                self._sinks_to_clear.append(self._sinks_active.pop(stream_name))
                return self.add_sqlsink(stream_name, schema, key_properties)

        return existing_sink

SQLTarget.get_sink = overriden_get_sink

class TargetSnowflake(SQLTarget):
    """Target for Snowflake."""

    name = "target-snowflake"
    package_name = "meltanolabs_target_snowflake"

    # From https://docs.snowflake.com/en/user-guide/sqlalchemy.html#connection-parameters
    config_jsonschema = th.PropertiesList(
        th.Property(
            "user",
            th.StringType,
            required=True,
            description="The login name for your Snowflake user.",
        ),
        th.Property(
            "password",
            th.StringType,
            required=False,
            description="The password for your Snowflake user.",
        ),
        th.Property(
            "private_key",
            th.StringType,
            required=False,
            secret=True,
            description=(
                "The private key contents, in PEM or base64-encoding format. "
                "For KeyPair authentication either `private_key` or `private_key_path` "
                "must be provided."
            ),
        ),
        th.Property(
            "private_key_path",
            th.StringType,
            required=False,
            description=(
                "Path to file containing private key. For KeyPair authentication either "
                "private_key or private_key_path must be provided."
            ),
        ),
        th.Property(
            "private_key_passphrase",
            th.StringType,
            required=False,
            description="Passphrase to decrypt private key if encrypted.",
        ),
        th.Property(
            "account",
            th.StringType,
            required=True,
            description="Your account identifier. See [Account Identifiers](https://docs.snowflake.com/en/user-guide/admin-account-identifier.html).",
        ),
        th.Property(
            "database",
            th.StringType,
            required=True,
            description="The initial database for the Snowflake session.",
        ),
        th.Property(
            "schema",
            th.StringType,
            description="The initial schema for the Snowflake session.",
        ),
        th.Property(
            "warehouse",
            th.StringType,
            description="The initial warehouse for the session.",
        ),
        th.Property(
            "role",
            th.StringType,
            description="The initial role for the session.",
        ),
        th.Property(
            "add_record_metadata",
            th.BooleanType,
            default=True,
            description="Whether to add metadata columns.",
        ),
        th.Property(
            "clean_up_batch_files",
            th.BooleanType,
            default=True,
            description="Whether to remove batch files after processing.",
        ),
        th.Property(
            "use_browser_authentication",
            th.BooleanType,
            default=False,
            description="Whether to use SSO authentication using an external browser.",
        ),
        th.Property(
            "timestamp_type",
            th.StringType,
            allowed_values=[t.name for t in SnowflakeTimestampType],
            default=DEFAULT_TIMESTAMP_TYPE.name,
            description="Snowflake timestamp type to use for date-time properties.",
        ),
    ).to_dict()

    default_sink_class = SnowflakeSink

    @classmethod
    def cb_inititalize(
        cls: type[TargetSnowflake],
        ctx: click.Context,
        param: click.Option,  # noqa: ARG003
        value: bool,  # noqa: FBT001
    ) -> None:
        if value:
            initializer()
            ctx.exit()

    @classmethod
    def get_singer_command(cls: type[TargetSnowflake]) -> click.Command:
        """Execute standard CLI handler for targets.

        Returns:
            A click.Command object.
        """
        command = super().get_singer_command()
        command.params.extend(
            [
                click.Option(
                    ["--initialize"],
                    is_flag=True,
                    help="Interactive Snowflake account initialization.",
                    callback=cls.cb_inititalize,
                    expose_value=False,
                ),
            ],
        )

        return command


if __name__ == "__main__":
    TargetSnowflake.cli()
