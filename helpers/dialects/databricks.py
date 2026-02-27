"""Databricks SQL dialect adapter (Spark SQL / Unity Catalog).

Databricks SQL is Spark SQL with ANSI SQL extensions. Key differences:
- Unity Catalog uses 3-part names: catalog.schema.table (backtick-quoted)
- DATE_TRUNC has the unit first (uppercase), same order as Snowflake
- DATEDIFF(end, start) returns integer days; use MONTHS_BETWEEN for months/years
- COLLECT_LIST + CONCAT_WS for string aggregation (no STRING_AGG)
- RAND() for random sampling (not RANDOM())
- DESCRIBE TABLE returns col_name / data_type / comment rows
"""

from __future__ import annotations

from helpers.dialects.base import SQLDialect


class DatabricksDialect(SQLDialect):
    """SQL dialect for Databricks SQL Warehouses (Unity Catalog)."""

    name: str = "databricks"

    # ------------------------------------------------------------------
    # Table qualification
    # ------------------------------------------------------------------

    def qualify_table(self, table: str, schema: str | None = None) -> str:
        """Return a backtick-quoted Unity Catalog table reference.

        *schema* may be ``catalog.schema`` (two-part) or just ``schema``.
        Parts are backtick-quoted to handle reserved words and mixed case.

        >>> DatabricksDialect().qualify_table('sessions', 'main.analytics')
        '`main`.`analytics`.`sessions`'
        >>> DatabricksDialect().qualify_table('sessions', 'analytics')
        '`analytics`.`sessions`'
        >>> DatabricksDialect().qualify_table('sessions')
        '`sessions`'
        """
        if schema:
            parts = schema.split(".")
            parts.append(table)
            return ".".join(f"`{p}`" for p in parts)
        return f"`{table}`"

    # limit_clause — inherited (LIMIT N)

    # ------------------------------------------------------------------
    # Date / time functions
    # ------------------------------------------------------------------

    def date_trunc(self, field: str, unit: str) -> str:
        """Databricks DATE_TRUNC — uppercase unit first, then field.

        >>> DatabricksDialect().date_trunc('event_date', 'month')
        "DATE_TRUNC('MONTH', event_date)"
        """
        return f"DATE_TRUNC('{unit.upper()}', {field})"

    def date_diff(self, unit: str, start: str, end: str) -> str:
        """Databricks date difference.

        DATEDIFF(end, start) returns integer days.
        For months, uses MONTHS_BETWEEN(end, start) cast to integer.
        For years, divides MONTHS_BETWEEN by 12.

        >>> DatabricksDialect().date_diff('day', 'start_date', 'end_date')
        'DATEDIFF(end_date, start_date)'
        >>> DatabricksDialect().date_diff('month', 'start_date', 'end_date')
        'CAST(MONTHS_BETWEEN(end_date, start_date) AS INT)'
        >>> DatabricksDialect().date_diff('year', 'start_date', 'end_date')
        'CAST(MONTHS_BETWEEN(end_date, start_date) / 12 AS INT)'
        """
        u = unit.lower()
        if u == "month":
            return f"CAST(MONTHS_BETWEEN({end}, {start}) AS INT)"
        elif u == "year":
            return f"CAST(MONTHS_BETWEEN({end}, {start}) / 12 AS INT)"
        else:
            # days, hours, minutes, seconds — DATEDIFF returns days
            return f"DATEDIFF({end}, {start})"

    # ------------------------------------------------------------------
    # Safe math
    # ------------------------------------------------------------------

    def safe_divide(self, numerator: str, denominator: str) -> str:
        """Databricks safe division — returns NULL on zero or NULL denominator.

        >>> DatabricksDialect().safe_divide('revenue', 'orders')
        'CASE WHEN orders = 0 OR orders IS NULL THEN NULL ELSE revenue / orders END'
        """
        return (
            f"CASE WHEN {denominator} = 0 OR {denominator} IS NULL "
            f"THEN NULL ELSE {numerator} / {denominator} END"
        )

    # ------------------------------------------------------------------
    # String aggregation
    # ------------------------------------------------------------------

    def string_agg(self, column: str, delimiter: str = ",") -> str:
        """Databricks string aggregation via COLLECT_LIST + CONCAT_WS.

        >>> DatabricksDialect().string_agg('category')
        "CONCAT_WS(',', COLLECT_LIST(category))"
        """
        return f"CONCAT_WS('{delimiter}', COLLECT_LIST({column}))"

    # current_timestamp — inherited (CURRENT_TIMESTAMP)

    # ------------------------------------------------------------------
    # Temp tables
    # ------------------------------------------------------------------

    def create_temp_table(self, name: str, query: str) -> str:
        """Databricks temporary view (session-scoped).

        Databricks recommends CREATE OR REPLACE TEMP VIEW for ad hoc work.

        >>> DatabricksDialect().create_temp_table('tmp_agg', 'SELECT 1')
        'CREATE OR REPLACE TEMP VIEW tmp_agg AS (SELECT 1)'
        """
        return f"CREATE OR REPLACE TEMP VIEW {name} AS ({query})"

    # ------------------------------------------------------------------
    # Sampling
    # ------------------------------------------------------------------

    def sample_rows(self, table: str, n: int) -> str:
        """Databricks random sample using ORDER BY RAND().

        >>> DatabricksDialect().sample_rows('sessions', 100)
        'SELECT * FROM sessions ORDER BY RAND() LIMIT 100'
        """
        return f"SELECT * FROM {table} ORDER BY RAND() LIMIT {int(n)}"

    # ------------------------------------------------------------------
    # Schema introspection
    # ------------------------------------------------------------------

    def describe_table(self, table: str) -> str:
        """Databricks DESCRIBE TABLE — returns col_name, data_type, comment.

        Note: rows where col_name starts with '#' are partition metadata
        and should be skipped when parsing results.

        >>> DatabricksDialect().describe_table('main.analytics.sessions')
        'DESCRIBE TABLE main.analytics.sessions'
        """
        return f"DESCRIBE TABLE {table}"
