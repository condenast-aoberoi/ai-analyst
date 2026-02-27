# Skill: Connect Data

## Purpose
Guided wizard to connect a new dataset. Walks the user through selecting
a connection type, configuring credentials, validating the connection,
profiling the schema, and setting up the knowledge brain.

## When to Use
- User says `/connect-data` or "connect my database" or "add a new dataset"
- First-run welcome suggests connecting data
- After `/switch-dataset` when the target dataset doesn't exist yet

## Invocation
`/connect-data` — start the connection wizard
`/connect-data type=postgres` — skip type selection

## Instructions

### Step 1: Choose Connection Type
Present options:
1. **CSV files** — "I have CSV files in a local directory"
2. **DuckDB** — "I have a local DuckDB database file"
3. **MotherDuck** — "I have a MotherDuck cloud database"
4. **PostgreSQL** — "I have a PostgreSQL database"
5. **BigQuery** — "I have a Google BigQuery dataset"
6. **Snowflake** — "I have a Snowflake warehouse"
7. **Databricks** — "I have a Databricks SQL Warehouse (Unity Catalog)"

### Step 2: Collect Connection Details

**For CSV:**
- Ask: "What's the path to your CSV directory? (relative to this repo)"
- Verify the directory exists and contains .csv files
- List found files and ask to confirm

**For DuckDB:**
- Ask: "Path to your .duckdb file?"
- Verify file exists
- Test connection with `SELECT 1`

**For MotherDuck:**
- Ask: "Database name and schema?"
- Note: "MotherDuck connects via MCP. Make sure your token is configured."

**For PostgreSQL / BigQuery / Snowflake:**
- Copy the appropriate template from `connection_templates/`
- Ask user to fill in required fields
- **IMPORTANT:** Never ask for or store passwords directly. Guide the user
  to use environment variables (e.g., `$PG_PASSWORD`).

**For Databricks:**

Prompt for connection details one at a time:

1. Ask: "What is your Databricks workspace hostname?"
   - No `https://` prefix — just the hostname
   - Azure: `{workspace}.azuredatabricks.net`
   - AWS: `{workspace}.cloud.databricks.com`
   - GCP: `{workspace}.gcp.databricks.com`
   - Where to find it: top-right of your Databricks workspace URL

2. Ask: "What is the HTTP path for your SQL Warehouse?"
   - Format: `/sql/1.0/warehouses/{id}`
   - Where to find it: Databricks UI → SQL Warehouses → select warehouse → Connection Details → HTTP Path

3. Ask: "What Unity Catalog catalog should I connect to?" (e.g., `main`)

4. Ask: "What schema within that catalog?" (e.g., `default`, `analytics`)

5. Guide auth setup with a decision tree — ask each question in sequence:

   **Q: "Is the Databricks CLI installed on this machine?"** (`databricks --version` to check)
   - **Yes** → Ask: "Do you have a profile already configured in `~/.databrickscfg`?"
     - **Yes** → Ask: "What is the profile name?" → store as `cli_profile: "{name}"` in manifest
     - **No** → Instruct: Run `databricks auth login --host https://{host}` — it will prompt
       for your workspace and save a profile. Ask for the profile name once complete.
   - **No** → Ask: "Do you have a Personal Access Token (PAT)?"
     - **Yes** → Instruct: `export DATABRICKS_TOKEN=dapi...` in shell before launching Claude.
       Store `token: "$DATABRICKS_TOKEN"` in manifest. Never store the raw token.
     - **No** → Ask: "Do you have OAuth M2M client credentials (client ID + secret)?"
       - **Yes** → Instruct: Set `export DATABRICKS_CLIENT_ID=...` and
         `export DATABRICKS_CLIENT_SECRET=...` in shell. Store env var references in manifest.
       - **No** → Recommend installing the Databricks CLI (`brew install databricks` or
         https://docs.databricks.com/dev-tools/cli/install.html) and running
         `databricks auth login --host https://{host}`.

After collecting details and creating the manifest, **check MCP server status:**
- Read `~/.claude/mcp.json` — look for a `"databricks"` key under `mcpServers`
- If **not present**, instruct the user to install the AI Dev Kit:
  ```
  bash <(curl -sL https://raw.githubusercontent.com/databricks-solutions/ai-dev-kit/main/install.sh)
  ```
  The installer clones the AI Dev Kit repo, creates its own venv, and writes the MCP config
  to `~/.claude/mcp.json`. Restart Claude Code after running it.
- If **already present**: MCP is configured. No further action needed.

### Step 3: Create Dataset Brain
1. Generate a dataset_id from the display name (lowercase, hyphens)
2. Create `.knowledge/datasets/{id}/` directory
3. Write `manifest.yaml` from the connection template + user inputs
4. Create empty `quirks.md` with section headers
5. Create empty `metrics/index.yaml`

### Step 4: Test Connection
Use `ConnectionManager` from `helpers/connection_manager.py`:
1. Instantiate with the new config
2. Call `test_connection()`
3. If fails: show error, offer to retry or edit config
4. If passes: proceed

### Step 5: Profile Schema
1. Call `list_tables()` to enumerate tables
2. For each table: get column names and types via `get_table_schema()`
3. Generate `schema.md` using `schema_to_markdown()` from `helpers/data_helpers.py`
4. Write to `.knowledge/datasets/{id}/schema.md`
5. Offer to run full data profiling: "Want me to deep-profile this dataset?"

### Step 6: Set Active
1. Update `.knowledge/active.yaml` to point to the new dataset
2. Confirm: "Connected! **{display_name}** is now your active dataset."
3. Show: table count, estimated row count, date range (if detected)
4. Suggest next steps: `/explore` to browse, `/metrics` to define metrics,
   or just ask a question

## Rules
1. Never store credentials in plain text in manifest files
2. Always test the connection before declaring success
3. Always generate a schema.md — it's required for analysis
4. Create the full .knowledge/datasets/{id}/ tree even if profiling fails
5. If the user already has this dataset, ask before overwriting

## Edge Cases
- **Directory doesn't exist:** Offer to create it
- **No CSV files found:** Check for other formats (.parquet, .json)
- **Connection fails repeatedly:** Suggest checking credentials, firewall, VPN
- **Schema too large (>100 tables):** Profile only, skip per-table details
- **Dataset name collision:** Append a number (e.g., "mydata-2")
