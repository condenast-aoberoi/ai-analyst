"""
Connection Manager — unified interface for multi-warehouse connections.

Manages connection lifecycle for different data warehouse backends:
MotherDuck/DuckDB (native), PostgreSQL, BigQuery, Snowflake, and Databricks.

Usage:
    from helpers.connection_manager import ConnectionManager

    mgr = ConnectionManager()
    conn = mgr.connect()          # Uses active dataset config
    mgr.test_connection()          # Health check
    tables = mgr.list_tables()     # Enumerate tables
    mgr.close()                    # Cleanup

    # Or use as context manager:
    with ConnectionManager() as mgr:
        tables = mgr.list_tables()
"""

import os
from pathlib import Path

import pandas as pd

# Optional imports — each warehouse backend is optional.
try:
    import duckdb
    _DUCKDB_AVAILABLE = True
except ImportError:
    _DUCKDB_AVAILABLE = False

try:
    import yaml
    _YAML_AVAILABLE = True
except ImportError:
    _YAML_AVAILABLE = False

try:
    from databricks import sql as _dbsql  # noqa: F401
    _DATABRICKS_AVAILABLE = True
except ImportError:
    _DATABRICKS_AVAILABLE = False


# Supported connection types and their required packages.
SUPPORTED_TYPES = {
    "motherduck": {"package": "duckdb", "installed": _DUCKDB_AVAILABLE},
    "duckdb": {"package": "duckdb", "installed": _DUCKDB_AVAILABLE},
    "csv": {"package": None, "installed": True},
    "postgres": {"package": "psycopg2", "installed": False},
    "bigquery": {"package": "google-cloud-bigquery", "installed": False},
    "snowflake": {"package": "snowflake-connector-python", "installed": False},
    "databricks": {"package": "databricks-sql-connector", "installed": _DATABRICKS_AVAILABLE},
}


class ConnectionManager:
    """Unified connection manager for multi-warehouse data access.

    Reads connection config from the active dataset's manifest, connects
    to the appropriate backend, and provides a common interface for
    table listing, health checks, and query execution.

    Args:
        config: Optional connection config dict. If None, reads from
            the active dataset manifest via data_helpers.
        dataset_id: Optional dataset ID to connect to. If None, uses
            the active dataset.
    """

    def __init__(self, config=None, dataset_id=None):
        self._config = config
        self._dataset_id = dataset_id
        self._connection = None
        self._conn_type = None
        self._schema_prefix = ""
        self._csv_dir = None

        if config is None:
            self._config = self._load_config(dataset_id)

        self._conn_type = self._config.get("type", "csv")
        self._schema_prefix = self._config.get("schema_prefix", "")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    # ------------------------------------------------------------------
    # Config loading
    # ------------------------------------------------------------------

    @staticmethod
    def _load_config(dataset_id=None):
        """Load connection config from the knowledge system.

        Reads .knowledge/active.yaml (or uses dataset_id) and loads the
        dataset's manifest.yaml for connection details.

        Returns:
            dict with connection configuration.
        """
        try:
            from helpers.data_helpers import detect_active_source
            source = detect_active_source()
            return {
                "type": source.get("type", "csv"),
                "dataset_id": source.get("source", "unknown"),
                "display_name": source.get("display_name", "Unknown"),
                "schema_prefix": source.get("schema_prefix", ""),
                "duckdb_path": source.get("duckdb_path"),
                "csv_path": source.get("csv_path"),
                "connection": source.get("connection", {}),
            }
        except Exception as exc:
            raise RuntimeError(
                "Failed to load dataset config — no active dataset found. "
                "Use /connect-data to configure a dataset, or pass a config dict "
                f"to ConnectionManager directly. Original error: {exc}"
            )

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def connect(self):
        """Establish connection to the configured data source.

        Returns:
            self (for chaining).

        Raises:
            ConnectionError: If the connection cannot be established.
        """
        conn_type = self._conn_type

        if conn_type in ("motherduck", "duckdb"):
            self._connect_duckdb()
        elif conn_type == "postgres":
            self._connect_postgres()
        elif conn_type == "bigquery":
            self._connect_bigquery()
        elif conn_type == "snowflake":
            self._connect_snowflake()
        elif conn_type == "databricks":
            self._connect_databricks()
        elif conn_type == "csv":
            self._connect_csv()
        else:
            raise ConnectionError(
                f"Unsupported connection type: {conn_type}. "
                f"Supported types: {list(SUPPORTED_TYPES.keys())}"
            )

        return self

    def close(self):
        """Close the active connection and release resources."""
        if self._connection is not None:
            try:
                if hasattr(self._connection, "close"):
                    self._connection.close()
            except Exception:
                pass
            self._connection = None

    def test_connection(self):
        """Test connectivity with a lightweight probe.

        Returns:
            dict: {ok: bool, type: str, message: str}
        """
        try:
            if self._conn_type in ("motherduck", "duckdb"):
                if self._connection is None:
                    self.connect()
                self._connection.sql("SELECT 1").fetchone()
                return {"ok": True, "type": self._conn_type, "message": "Connected"}

            elif self._conn_type == "postgres":
                if self._connection is None:
                    self.connect()
                cur = self._connection.cursor()
                cur.execute("SELECT 1")
                cur.fetchone()
                cur.close()
                return {"ok": True, "type": "postgres", "message": "Connected"}

            elif self._conn_type == "databricks":
                if self._connection is None:
                    self.connect()
                cur = self._connection.cursor()
                cur.execute("SELECT 1")
                cur.fetchone()
                cur.close()
                return {"ok": True, "type": "databricks", "message": "Connected"}

            elif self._conn_type == "csv":
                csv_dir = self._csv_dir or self._config.get("csv_path", "")
                if Path(csv_dir).is_dir():
                    count = len(list(Path(csv_dir).glob("*.csv")))
                    return {"ok": count > 0, "type": "csv", "message": f"{count} CSV files"}
                return {"ok": False, "type": "csv", "message": f"Directory not found: {csv_dir}"}

            else:
                return {"ok": False, "type": self._conn_type, "message": "Not yet implemented"}

        except Exception as exc:
            return {"ok": False, "type": self._conn_type, "message": str(exc)}

    # ------------------------------------------------------------------
    # Table operations
    # ------------------------------------------------------------------

    def list_tables(self):
        """List all available tables in the connected source.

        Returns:
            list[str]: Sorted table names.
        """
        if self._conn_type in ("motherduck", "duckdb") and self._connection:
            try:
                df = self._connection.sql("SHOW TABLES").df()
                return sorted(df["name"].tolist()) if "name" in df.columns else []
            except Exception:
                return []

        elif self._conn_type == "postgres" and self._connection:
            try:
                cur = self._connection.cursor()
                schema = self._schema_prefix or "public"
                cur.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = %s ORDER BY table_name",
                    (schema,),
                )
                tables = [row[0] for row in cur.fetchall()]
                cur.close()
                return tables
            except Exception:
                return []

        elif self._conn_type == "databricks" and self._connection:
            try:
                cfg = self._config.get("connection", {})
                catalog = cfg.get("catalog", "")
                schema = cfg.get("schema", "")
                cur = self._connection.cursor()
                cur.execute(f"SHOW TABLES IN `{catalog}`.`{schema}`")
                rows = cur.fetchall()
                cur.close()
                # SHOW TABLES returns rows with (database, tableName, isTemporary)
                # column index 1 is tableName
                return sorted(row[1] for row in rows)
            except Exception:
                return []

        elif self._conn_type == "csv":
            csv_dir = self._csv_dir or self._config.get("csv_path", "")
            if Path(csv_dir).is_dir():
                return sorted(p.stem for p in Path(csv_dir).glob("*.csv"))
            return []

        return []

    def get_table_schema(self, table_name):
        """Get column names and types for a specific table.

        Args:
            table_name: Name of the table to inspect.

        Returns:
            list[dict]: Each dict has keys: name, type, nullable.
        """
        if self._conn_type in ("motherduck", "duckdb") and self._connection:
            try:
                df = self._connection.sql(f"DESCRIBE {table_name}").df()
                columns = []
                for _, row in df.iterrows():
                    columns.append({
                        "name": row.get("column_name", row.get("Field", "")),
                        "type": row.get("column_type", row.get("Type", "")),
                        "nullable": row.get("null", "YES") == "YES",
                    })
                return columns
            except Exception:
                return []

        elif self._conn_type == "databricks" and self._connection:
            try:
                fqn = f"{self._schema_prefix}.{table_name}" if self._schema_prefix else table_name
                cur = self._connection.cursor()
                cur.execute(f"DESCRIBE TABLE {fqn}")
                rows = cur.fetchall()
                cur.close()
                columns = []
                for row in rows:
                    col_name = row[0]
                    # Skip partition metadata rows (start with '#')
                    if col_name.startswith("#") or col_name.strip() == "":
                        continue
                    columns.append({
                        "name": col_name,
                        "type": row[1] if len(row) > 1 else "unknown",
                        "nullable": True,
                    })
                return columns
            except Exception:
                return []

        elif self._conn_type == "csv":
            csv_dir = self._csv_dir or self._config.get("csv_path", "")
            csv_path = Path(csv_dir) / f"{table_name}.csv"
            if csv_path.exists():
                df = pd.read_csv(csv_path, nrows=5, low_memory=False)
                return [
                    {"name": col, "type": str(df[col].dtype), "nullable": True}
                    for col in df.columns
                ]
            return []

        return []

    def query(self, sql):
        """Execute a SQL query and return results as a DataFrame.

        Args:
            sql: SQL query string.

        Returns:
            pandas.DataFrame with query results.

        Raises:
            RuntimeError: If no SQL-capable connection is available.
        """
        if self._conn_type in ("motherduck", "duckdb") and self._connection:
            return self._connection.sql(sql).df()

        elif self._conn_type == "postgres" and self._connection:
            return pd.read_sql(sql, self._connection)

        elif self._conn_type == "databricks" and self._connection:
            cur = self._connection.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description] if cur.description else []
            cur.close()
            return pd.DataFrame(rows, columns=columns)

        raise RuntimeError(
            f"SQL queries not supported for connection type: {self._conn_type}. "
            "Use read_table() for CSV data."
        )

    def read_table(self, table_name):
        """Read an entire table as a DataFrame.

        Works for all connection types including CSV.

        Args:
            table_name: Name of the table to read.

        Returns:
            pandas.DataFrame
        """
        if self._conn_type in ("motherduck", "duckdb") and self._connection:
            return self._connection.sql(f"SELECT * FROM {table_name}").df()

        elif self._conn_type == "csv":
            csv_dir = self._csv_dir or self._config.get("csv_path", "")
            csv_path = Path(csv_dir) / f"{table_name}.csv"
            if csv_path.exists():
                return pd.read_csv(csv_path, low_memory=False)
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        elif self._conn_type == "postgres" and self._connection:
            schema = self._schema_prefix or "public"
            return pd.read_sql(f"SELECT * FROM {schema}.{table_name}", self._connection)

        elif self._conn_type == "databricks" and self._connection:
            fqn = f"{self._schema_prefix}.{table_name}" if self._schema_prefix else table_name
            return self.query(f"SELECT * FROM {fqn}")

        raise RuntimeError(f"Cannot read table for connection type: {self._conn_type}")

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def connection_type(self):
        """Return the active connection type string."""
        return self._conn_type

    @property
    def schema_prefix(self):
        """Return the SQL schema prefix for the active connection."""
        return self._schema_prefix

    @property
    def is_connected(self):
        """Return True if a connection is currently active."""
        if self._conn_type == "csv":
            csv_dir = self._csv_dir or self._config.get("csv_path", "")
            return Path(csv_dir).is_dir()
        return self._connection is not None

    @property
    def dataset_id(self):
        """Return the active dataset ID."""
        return self._config.get("dataset_id", self._dataset_id or "unknown")

    # ------------------------------------------------------------------
    # Backend-specific connectors (private)
    # ------------------------------------------------------------------

    def _connect_duckdb(self):
        """Connect to local DuckDB or MotherDuck fallback."""
        if not _DUCKDB_AVAILABLE:
            raise ConnectionError("duckdb package not installed. pip install duckdb")

        db_path = self._config.get("duckdb_path")
        if db_path and Path(db_path).exists():
            self._connection = duckdb.connect(str(db_path), read_only=True)
            self._conn_type = "duckdb"
        else:
            # Fallback to CSV
            self._conn_type = "csv"
            self._connect_csv()

    def _connect_csv(self):
        """Set up CSV-based access."""
        csv_path = self._config.get("csv_path")
        if not csv_path:
            raise ConnectionError(
                "No csv_path configured for CSV connection. "
                "Set csv_path in the dataset manifest or pass it in the config dict."
            )
        self._csv_dir = csv_path
        self._conn_type = "csv"

    def _connect_postgres(self):
        """Connect to PostgreSQL. Requires psycopg2."""
        try:
            import psycopg2
        except ImportError:
            raise ConnectionError(
                "psycopg2 not installed. Install with: pip install psycopg2-binary"
            )

        conn_config = self._config.get("connection", {})
        self._connection = psycopg2.connect(
            host=conn_config.get("host", "localhost"),
            port=conn_config.get("port", 5432),
            database=conn_config.get("database", ""),
            user=conn_config.get("user", ""),
            password=conn_config.get("password", ""),
        )
        self._schema_prefix = conn_config.get("schema", "public")

    def _connect_bigquery(self):
        """Connect to BigQuery. Requires google-cloud-bigquery."""
        try:
            from google.cloud import bigquery
        except ImportError:
            raise ConnectionError(
                "google-cloud-bigquery not installed. "
                "Install with: pip install google-cloud-bigquery"
            )

        conn_config = self._config.get("connection", {})
        project = conn_config.get("project")
        self._connection = bigquery.Client(project=project)
        self._schema_prefix = conn_config.get("dataset", "")
        self._conn_type = "bigquery"

    def _connect_snowflake(self):
        """Connect to Snowflake. Requires snowflake-connector-python."""
        try:
            import snowflake.connector
        except ImportError:
            raise ConnectionError(
                "snowflake-connector-python not installed. "
                "Install with: pip install snowflake-connector-python"
            )

        conn_config = self._config.get("connection", {})
        self._connection = snowflake.connector.connect(
            account=conn_config.get("account", ""),
            user=conn_config.get("user", ""),
            password=conn_config.get("password", ""),
            warehouse=conn_config.get("warehouse", ""),
            database=conn_config.get("database", ""),
            schema=conn_config.get("schema", "public"),
        )
        self._schema_prefix = conn_config.get("schema", "public")
        self._conn_type = "snowflake"

    def _connect_databricks(self):
        """Connect to a Databricks SQL Warehouse. Requires databricks-sql-connector.

        Auth priority (first match wins):
        1. ``cli_profile`` key in connection config → token fetched via
           ``databricks auth token`` CLI command (no secrets in manifest).
        2. ``token`` key → PAT token or ``$ENV_VAR`` reference.
        3. ``client_id`` / ``client_secret`` keys → OAuth M2M env var references.
        """
        if not _DATABRICKS_AVAILABLE:
            raise ConnectionError(
                "databricks-sql-connector not installed. "
                "Install with: pip install databricks-sql-connector"
            )

        from databricks import sql as dbsql

        conn_config = self._config.get("connection", {})
        host = conn_config.get("host", "")

        # --- Auth resolution ---
        auth = {}
        if "cli_profile" in conn_config:
            # Resolve token via Databricks CLI — no secrets stored in manifest.
            import json
            import subprocess
            profile = conn_config["cli_profile"]
            result = subprocess.run(
                ["databricks", "auth", "token",
                 "--host", f"https://{host}",
                 "--profile", profile],
                capture_output=True, text=True,
            )
            if result.returncode != 0:
                raise ConnectionError(
                    f"Databricks CLI auth failed for profile '{profile}': "
                    f"{result.stderr.strip()}. "
                    f"Run: databricks auth login --host https://{host} --profile {profile}"
                )
            auth["access_token"] = json.loads(result.stdout)["access_token"]

        elif "token" in conn_config:
            # PAT token — stored as $ENV_VAR reference or raw value.
            raw = conn_config["token"]
            env_key = raw.lstrip("$")
            auth["access_token"] = os.environ.get(env_key, raw)

        elif "client_id" in conn_config:
            # OAuth M2M — stored as $ENV_VAR references.
            raw_id = conn_config["client_id"]
            raw_secret = conn_config.get("client_secret", "")
            auth["client_id"] = os.environ.get(raw_id.lstrip("$"), raw_id)
            auth["client_secret"] = os.environ.get(raw_secret.lstrip("$"), raw_secret)

        else:
            raise ConnectionError(
                "Databricks connection requires one of: 'cli_profile' (Databricks CLI), "
                "'token' (PAT), or 'client_id'/'client_secret' (OAuth M2M) "
                "in the connection config."
            )

        self._connection = dbsql.connect(
            server_hostname=host,
            http_path=conn_config.get("http_path", ""),
            **auth,
        )
        catalog = conn_config.get("catalog", "")
        schema = conn_config.get("schema", "")
        self._schema_prefix = f"{catalog}.{schema}" if catalog and schema else schema
        self._conn_type = "databricks"
