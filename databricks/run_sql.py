"""
Runs a .sql file against the Databricks Serverless SQL Warehouse.

We use this instead of PySpark notebooks because Databricks Free Edition
doesn't give you an all-purpose cluster - but it does give you a Serverless
SQL Warehouse, which can run all our Bronze and Silver SQL.

Each statement in the file (separated by semicolons) is sent to the warehouse
using the SQL Statement Execution API.

Usage:
    python databricks/run_sql.py databricks/sql/01_bronze.sql
"""

import os
import sys
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


def split_statements(sql_text):
    """
    Split a .sql file into individual statements on semicolons.

    We strip out "--" comments first, so a stray semicolon inside a comment
    can't accidentally break a statement in half. (Our SQL never has "--"
    inside a string literal, so cutting at "--" is safe here.)
    """
    # Remove comments line by line
    clean_lines = []
    for line in sql_text.splitlines():
        if "--" in line:
            line = line[:line.index("--")]
        clean_lines.append(line)
    clean_sql = "\n".join(clean_lines)

    # Now split on semicolons and drop empty chunks
    statements = [s.strip() for s in clean_sql.split(";") if s.strip()]
    return statements


def run_statement(client, warehouse_id, statement):
    """Run one SQL statement and wait for it to finish."""
    resp = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        wait_timeout="30s",
    )

    # If it's still running after 30s, poll until it's done
    state = resp.status.state
    while state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(2)
        resp = client.statement_execution.get_statement(resp.statement_id)
        state = resp.status.state

    if state != StatementState.SUCCEEDED:
        error_msg = resp.status.error.message if resp.status.error else "unknown error"
        raise Exception(f"Statement failed ({state}): {error_msg}")


def main():
    if len(sys.argv) != 2:
        print("Usage: python run_sql.py <path_to_sql_file>")
        sys.exit(1)

    sql_file = sys.argv[1]

    host  = os.environ["DATABRICKS_HOST"]
    token = os.environ["DATABRICKS_TOKEN"]

    # The warehouse ID is the last part of the HTTP path:
    # /sql/1.0/warehouses/5b6dcbc812fb1238  ->  5b6dcbc812fb1238
    http_path    = os.environ["DATABRICKS_HTTP_PATH"]
    warehouse_id = http_path.rstrip("/").split("/")[-1]

    with open(sql_file, "r") as f:
        sql_text = f.read()

    statements = split_statements(sql_text)
    print(f"Running {len(statements)} statements from {sql_file}")

    client = WorkspaceClient(host=host, token=token)

    for i, statement in enumerate(statements, 1):
        # Print the first real (non-comment) line so the logs show what's running
        sql_lines = [
            line.strip() for line in statement.splitlines()
            if line.strip() and not line.strip().startswith("--")
        ]
        preview = sql_lines[0][:80] if sql_lines else statement[:80]
        print(f"  [{i}/{len(statements)}] {preview}...")
        run_statement(client, warehouse_id, statement)

    print("All statements completed successfully.")


if __name__ == "__main__":
    main()
